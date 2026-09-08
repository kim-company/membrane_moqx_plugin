# MIX_ENV=test mise exec -- mix run scripts/interop/publish_epochs.exs
# check-epochs.mjs sends each `next` only after 50 actual decoded AudioData outputs.
defmodule Membrane.MOQX.Interop.EpochPublisher do
  import Membrane.ChildrenSpec
  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.Event.EmptyGroup
  alias Membrane.MOQX.Hang.Legacy
  alias Membrane.MOQX.{Sink, TestControlledSource, Track, Unit}
  alias Membrane.Testing
  require Pad

  def run do
    Logger.configure(level: :warning)
    path = Path.join(System.fetch_env!("MOQX_INTEROP_MEDIA"), "audio.ogg")

    {json, 0} =
      System.cmd("ffprobe", ["-v", "error", "-show_packets", "-show_data", "-of", "json", path])

    probe = JSON.decode!(json)
    first = timestamp(hd(probe["packets"]))

    packets =
      probe["packets"] |> Enum.take(50) |> Enum.map(&{timestamp(&1) - first, bytes(&1["data"])})

    if length(packets) != 50, do: raise("fixture requires at least 50 Opus packets")
    namespace = String.split(System.fetch_env!("MOQX_INTEROP_BROADCAST"), "/")

    format = %Track{
      packaging: "opus",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    spec =
      child(:source, %TestControlledSource{stream_format: format})
      |> child(:framing, %Legacy{direction: :encode})
      |> via_in(Pad.ref(:input, :audio),
        options: [track_name: "audio", timescale: 1_000_000, publisher_max_latency: 30_000_000]
      )
      |> child(:sink, %Sink{
        endpoint: System.fetch_env!("MOQX_LITE_ENDPOINT"),
        protocol: :moq_lite_05,
        profile: :hang,
        namespace: namespace,
        connect_options: [
          verify: :verify_peer,
          cacertfile: System.fetch_env!("MOQX_LITE_CA_FILE")
        ]
      })

    {:ok, _supervisor, pipeline} = Testing.Pipeline.start(spec: spec)
    monitor = Process.monitor(pipeline)
    await(pipeline, monitor, :ready)
    IO.puts("READY")
    await(pipeline, monitor, :subscriber)

    for {offset, epoch} <- Enum.with_index([5_000_000, 10_000_000, 0]) do
      if epoch > 0, do: Testing.Pipeline.notify_child(pipeline, :source, {:event, %EmptyGroup{}})
      start = System.monotonic_time(:microsecond)

      for {{pts, payload}, index} <- Enum.with_index(packets) do
        # Fixture media pacing only; receipt is acknowledged by the decoder below.
        remaining = pts - (System.monotonic_time(:microsecond) - start)
        if remaining > 0, do: Process.sleep(div(remaining, 1_000))

        buffer = %Buffer{
          payload: payload,
          pts: (pts + offset) * 1_000,
          metadata: %{moqx: %Unit{group_end?: index == 49}}
        }

        Testing.Pipeline.notify_child(pipeline, :source, {:publish, [buffer]})
      end

      IO.puts("EPOCH #{epoch}")
      unless IO.gets("") == "next\n", do: raise("missing actual decoder phase acknowledgement")
    end

    Testing.Pipeline.terminate(pipeline)
  end

  defp await(pipeline, monitor, phase) do
    receive do
      {Testing.Pipeline, ^pipeline,
       {:handle_child_notification, {{:publication_ready, _}, :sink}}}
      when phase == :ready ->
        :ok

      {Testing.Pipeline, ^pipeline,
       {:handle_child_notification, {{:subscriber_joined, "audio", _, _}, :sink}}}
      when phase == :subscriber ->
        :ok

      {:DOWN, ^monitor, :process, ^pipeline, reason} ->
        raise("pipeline failed: #{inspect(reason)}")
    after
      30_000 -> raise("publisher #{phase} timeout")
    end
  end

  defp timestamp(packet), do: round(String.to_float(packet["pts_time"]) * 1_000_000)

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

Membrane.MOQX.Interop.EpochPublisher.run()
