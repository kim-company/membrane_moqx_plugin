# MIX_ENV=test mise exec -- mix run scripts/interop/receive_cmaf.exs
# Receives synthetic reference media through public plugin pipelines, then
# independently decodes the captured CMAF with FFmpeg. Never use secret media.
defmodule Membrane.MOQX.Interop.CMAFReceiver do
  import Membrane.ChildrenSpec
  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.{CatalogSource, TrackOffer}
  alias Membrane.MOQX.TrackAdapter.FromTrack
  alias Membrane.Testing
  require Pad

  def run do
    Logger.configure(level: :warning)
    namespace = String.split(System.fetch_env!("MOQX_INTEROP_BROADCAST"), "/")

    {:ok, supervisor, pipeline} =
      Testing.Pipeline.start(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: System.fetch_env!("MOQX_LITE_ENDPOINT"),
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            connect_options: [
              verify: :verify_peer,
              cacertfile: System.fetch_env!("MOQX_LITE_CA_FILE")
            ]
          })
      )

    monitor = Process.monitor(supervisor)

    try do
      {formats, buffers} = receive_media(pipeline, monitor, %{}, %{}, %{})
      output = System.fetch_env!("MOQX_INTEROP_OUTPUT")

      Enum.each([:video, :audio], fn role ->
        packets = Enum.reverse(buffers[role])
        pts = Enum.map(packets, & &1.pts)
        true = Enum.all?(pts, &is_integer/1)
        true = pts == Enum.sort(pts)
        true = List.last(pts) > hd(pts)
        path = Path.join(output, "received-#{role}.mp4")
        File.write!(path, [formats[role].header | Enum.map(packets, & &1.payload)])

        IO.puts(
          "RECEIVED #{role} chunks=#{length(packets)} first_pts=#{hd(pts)} last_pts=#{List.last(pts)}"
        )
      end)

      video_path = Path.join(output, "received-video.mp4")

      {frames, 0} =
        System.cmd("ffmpeg", [
          "-v",
          "error",
          "-i",
          video_path,
          "-map",
          "0:v:0",
          "-f",
          "framemd5",
          "-"
        ])

      count =
        frames
        |> String.split("\n", trim: true)
        |> Enum.count(&(not String.starts_with?(&1, "#")))

      true = count >= 20
      audio_path = Path.join(output, "received-audio.mp4")

      {pcm, 0} =
        System.cmd("ffmpeg", [
          "-v",
          "error",
          "-i",
          audio_path,
          "-map",
          "0:a:0",
          "-f",
          "s16le",
          "-"
        ])

      samples = for <<sample::little-signed-16 <- pcm>>, do: abs(sample)
      peak = Enum.max(samples)
      true = length(samples) >= 96_000 and peak > 32

      IO.puts(
        "DECODE_PROOF video_frames=#{count} audio_samples=#{length(samples)} audio_peak_s16=#{peak}"
      )
    after
      Testing.Pipeline.terminate(pipeline)
    end
  end

  defp receive_media(pipeline, monitor, selected, formats, buffers) do
    if Enum.all?([:video, :audio], &(length(Map.get(buffers, &1, [])) >= 3)) do
      {formats, buffers}
    else
      receive do
        {Testing.Pipeline, ^pipeline,
         {:handle_child_notification, {{:track_available, %TrackOffer{} = offer}, :catalog}}} ->
          role =
            case offer.stream_format.catalog_fields["role"] do
              "video" -> :video
              "audio" -> :audio
            end

          selected =
            if Map.has_key?(selected, role) do
              selected
            else
              spec =
                get_child(:catalog)
                |> via_out(Pad.ref(:output, role),
                  options: [track: offer.track_ref, start_policy: :next_group]
                )
                |> child({:adapter, role}, %FromTrack{adapter: Membrane.MOQX.Hang.CMAF})
                |> child({:sink, role}, Testing.Sink)

              Testing.Pipeline.execute_actions(pipeline, spec: spec)
              Map.put(selected, role, offer.track_ref)
            end

          receive_media(pipeline, monitor, selected, formats, buffers)

        {Testing.Pipeline, ^pipeline,
         {:handle_child_notification,
          {{:stream_format, :input, %Membrane.CMAF.Track{} = format}, {:sink, role}}}} ->
          receive_media(pipeline, monitor, selected, Map.put(formats, role, format), buffers)

        {Testing.Pipeline, ^pipeline,
         {:handle_child_notification, {{:buffer, %Buffer{} = buffer}, {:sink, role}}}} ->
          receive_media(
            pipeline,
            monitor,
            selected,
            formats,
            Map.update(buffers, role, [buffer], &[buffer | &1])
          )

        {:DOWN, ^monitor, :process, _pid, reason} ->
          raise "receiver failed: #{inspect(reason)}"

        _message ->
          receive_media(pipeline, monitor, selected, formats, buffers)
      after
        15_000 -> raise "reference media not received"
      end
    end
  end
end

Membrane.MOQX.Interop.CMAFReceiver.run()
