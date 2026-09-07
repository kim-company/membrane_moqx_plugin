# Run with MIX_ENV=test mise exec -- mix run scripts/interop/publish_legacy.exs
# Generates no credentials. Supply synthetic fixtures and a task-local trusted CA.
defmodule Membrane.MOQX.Interop.LegacyPublisher do
  import Membrane.ChildrenSpec
  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.Hang.Legacy
  alias Membrane.MOQX.TrackAdapter.ToTrack
  alias Membrane.MOQX.{Sink, TestControlledSource, Track, Unit}
  alias Membrane.Testing
  require Pad

  def run do
    Logger.configure(level: :warning)
    media = System.fetch_env!("MOQX_INTEROP_MEDIA")
    cmaf? = System.get_env("MOQX_INTEROP_CONTAINER", "legacy") == "cmaf"
    video_path = Path.join(media, if(cmaf?, do: "video-cmaf.mp4", else: "video.mp4"))
    audio_path = Path.join(media, if(cmaf?, do: "audio-cmaf.mp4", else: "audio.ogg"))
    video = probe(video_path)
    audio = probe(audio_path)
    [video_stream] = video["streams"]
    description = bytes(video_stream["extradata"])
    <<1, profile, compatibility, level, _rest::binary>> = description
    codec = "avc1." <> Base.encode16(<<profile, compatibility, level>>, case: :lower)

    formats = [
      video: %Track{
        packaging: "h264",
        initialization: description,
        selection_params: %{
          "codec" => codec,
          "codedWidth" => video_stream["width"],
          "codedHeight" => video_stream["height"]
        }
      },
      audio: %Track{
        packaging: "opus",
        initialization: nil,
        selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
      }
    ]

    formats =
      if cmaf? do
        [
          video: %Membrane.CMAF.Track{
            content_type: :video,
            header: initialization(video_path),
            resolution: {video_stream["width"], video_stream["height"]},
            codecs: %{
              avc1: %{
                profile: Base.encode16(<<profile>>),
                compatibility: Base.encode16(<<compatibility>>),
                level: Base.encode16(<<level>>)
              }
            }
          },
          audio: %Membrane.CMAF.Track{
            content_type: :audio,
            header: initialization(audio_path),
            codecs: %{mp4a: %{aot_id: "2", channels: 2, frequency: 48_000}}
          }
        ]
      else
        formats
      end

    namespace = String.split(System.fetch_env!("MOQX_INTEROP_BROADCAST"), "/")

    spec = [
      child(:sink, %Sink{
        endpoint: System.fetch_env!("MOQX_LITE_ENDPOINT"),
        protocol: :moq_lite_05,
        profile: :hang,
        namespace: namespace,
        connect_options: [
          verify: :verify_peer,
          cacertfile: System.fetch_env!("MOQX_LITE_CA_FILE")
        ]
      })
      | Enum.map(formats, fn {name, format} ->
          child(name, %TestControlledSource{stream_format: format})
          |> child(
            {:framing, name},
            if(cmaf?,
              do: %ToTrack{adapter: Membrane.MOQX.Hang.CMAF},
              else: %Legacy{direction: :encode}
            )
          )
          |> via_in(Pad.ref(:input, name),
            options: [
              track_name: Atom.to_string(name),
              timescale: 1_000_000,
              publisher_max_latency: 30_000_000
            ]
          )
          |> get_child(:sink)
        end)
    ]

    {:ok, _supervisor, pipeline} = Testing.Pipeline.start(spec: spec)
    monitor = Process.monitor(pipeline)

    receive do
      {Testing.Pipeline, ^pipeline,
       {:handle_child_notification, {{:publication_ready, ^namespace}, :sink}}} ->
        :ok

      {:DOWN, ^monitor, :process, ^pipeline, reason} ->
        raise "publication failed: #{inspect(reason)}"
    after
      10_000 -> raise "publication not ready"
    end

    IO.puts("INTEROP_PUBLISHER_READY #{Enum.join(namespace, "/")}")
    await_subscribers(pipeline, MapSet.new())

    packets =
      if cmaf? do
        fragments(video_path, video, :video) ++ fragments(audio_path, audio, :audio)
      else
        packets(video, :video) ++ packets(audio, :audio)
      end

    packets = Enum.sort_by(packets, &elem(&1, 0))
    start = System.monotonic_time(:millisecond)
    # Pacing is the media clock, never a delivery acknowledgement.
    # CMAF contains absolute decode times: never loop fragments by changing only PTS.
    for cycle <- 0..if(cmaf?, do: 0, else: 4), {pts, name, buffer} <- packets do
      pts = pts + cycle * 4_000_000_000
      delay = div(pts, 1_000_000) - (System.monotonic_time(:millisecond) - start)
      if delay > 0, do: Process.sleep(delay)
      Testing.Pipeline.notify_child(pipeline, name, {:publish, [%{buffer | pts: pts}]})
    end

    IO.puts("INTEROP_PUBLISHER_MEDIA_SENT")
    # Keep the retained catalog/publication alive while the receiver records proof.
    receive do
      :stop -> :ok
    after
      30_000 -> :ok
    end

    Testing.Pipeline.terminate(pipeline)
  end

  defp initialization(path) do
    path
    |> File.read!()
    |> boxes()
    |> Enum.take_while(fn {type, _} -> type != "moof" end)
    |> Enum.map(&elem(&1, 1))
    |> IO.iodata_to_binary()
  end

  defp fragments(path, probe, name) do
    [stream] = probe["streams"]
    ["1", scale] = String.split(stream["time_base"], "/")
    scale = String.to_integer(scale)

    path
    |> File.read!()
    |> boxes()
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.flat_map(fn
      [{"moof", moof}, {"mdat", mdat}] ->
        <<_size::32, "moof", inner::binary>> = moof
        {"traf", <<_size::32, "traf", traf::binary>>} = List.keyfind(boxes(inner), "traf", 0)

        {"tfdt", <<_size::32, "tfdt", version, _flags::24, time::binary>>} =
          List.keyfind(boxes(traf), "tfdt", 0)

        ticks =
          case {version, time} do
            {0, <<ticks::32>>} -> ticks
            {1, <<ticks::64>>} -> ticks
          end

        pts = div(ticks * 1_000_000_000, scale)
        [{pts, name, %Buffer{payload: moof <> mdat, pts: pts, metadata: %{last_chunk?: true}}}]

      _other ->
        []
    end)
  end

  # Fixture-only ISO BMFF splitter; the plugin itself never parses these boxes.
  defp boxes(<<>>), do: []

  defp boxes(<<size::32, type::binary-size(4), rest::binary>>) when size >= 8 do
    <<body::binary-size(^size - 8), rest::binary>> = rest
    [{type, <<size::32, type::binary, body::binary>>} | boxes(rest)]
  end

  defp await_subscribers(pipeline, joined) do
    if MapSet.size(joined) < 2 do
      receive do
        {Testing.Pipeline, ^pipeline,
         {:handle_child_notification, {{:subscriber_joined, name, _, _}, :sink}}}
        when name in ["audio", "video"] ->
          await_subscribers(pipeline, MapSet.put(joined, name))

        _message ->
          await_subscribers(pipeline, joined)
      after
        60_000 -> raise "reference player did not subscribe to both media tracks"
      end
    end
  end

  defp probe(path) do
    {json, 0} =
      System.cmd("ffprobe", [
        "-v",
        "error",
        "-show_packets",
        "-show_streams",
        "-show_data",
        "-of",
        "json",
        path
      ])

    JSON.decode!(json)
  end

  defp packets(probe, name) do
    packets = probe["packets"]
    first = timestamp(hd(packets))

    Enum.with_index(packets, fn packet, index ->
      pts = timestamp(packet) - first
      keyframe? = String.contains?(packet["flags"], "K")
      next = Enum.at(packets, index + 1)
      last? = name == :audio or is_nil(next) or String.contains?(next["flags"], "K")

      {pts, name,
       %Buffer{
         payload: bytes(packet["data"]),
         pts: pts,
         metadata: %{keyframe?: keyframe?, moqx: %Unit{group_end?: last?}}
       }}
    end)
  end

  defp timestamp(packet), do: round(String.to_float(packet["pts_time"]) * 1_000_000_000)

  defp bytes(dump) do
    dump
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      [_offset, data] = String.split(line, ": ", parts: 2)

      data
      |> String.split("  ", parts: 2)
      |> hd()
      |> String.replace(" ", "")
      |> Base.decode16!(case: :mixed)
    end)
    |> IO.iodata_to_binary()
  end
end

Membrane.MOQX.Interop.LegacyPublisher.run()
