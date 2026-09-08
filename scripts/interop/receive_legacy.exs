# Synthetic reference media only. The companion browser check independently
# decodes these public pipeline outputs using WebCodecs.
defmodule Membrane.MOQX.Interop.LegacyReceiver do
  import Membrane.ChildrenSpec
  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.{CatalogSource, Hang.Legacy, Track, TrackOffer}
  alias Membrane.Testing
  require Pad

  def run do
    Logger.configure(level: :warning)

    {:ok, supervisor, pipeline} =
      Testing.Pipeline.start(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: System.fetch_env!("MOQX_LITE_ENDPOINT"),
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: String.split(System.fetch_env!("MOQX_INTEROP_BROADCAST"), "/"),
            connect_options: [
              verify: :verify_peer,
              cacertfile: System.fetch_env!("MOQX_LITE_CA_FILE")
            ]
          })
      )

    monitor = Process.monitor(supervisor)

    try do
      {formats, buffers} = collect(pipeline, monitor, %{}, %{}, %{})

      result =
        Map.new([:video, :audio], fn role ->
          packets = Enum.reverse(buffers[role])
          pts = Enum.map(packets, & &1.pts)
          true = Enum.all?(pts, &is_integer/1)
          # QUIC groups may interleave. Preserve arrival order in the capture;
          # validate only the ordering this Source actually promises.
          packets
          |> Enum.group_by(& &1.metadata.moqx.group_id)
          |> Enum.each(fn {_group, group_packets} ->
            objects = Enum.map(group_packets, & &1.metadata.moqx.object_id)
            true = objects == Enum.to_list(0..(length(objects) - 1))
            group_pts = Enum.map(group_packets, & &1.pts)
            true = group_pts == Enum.sort(group_pts)
          end)

          true = Enum.max(pts) > Enum.min(pts)
          format = formats[role]

          {role,
           %{
             config: format.selection_params,
             description: format.initialization && Base.encode64(format.initialization),
             packets:
               Enum.map(packets, fn buffer ->
                 %{
                   timestamp: div(buffer.pts, 1_000),
                   group: buffer.metadata.moqx.group_id,
                   object: buffer.metadata.moqx.object_id,
                   key: role == :audio or buffer.metadata[:keyframe?] == true,
                   data: Base.encode64(buffer.payload)
                 }
               end)
           }}
        end)

      File.write!(
        Path.join(System.fetch_env!("MOQX_INTEROP_OUTPUT"), "legacy.json"),
        Jason.encode!(result)
      )

      IO.puts("LEGACY_CAPTURE video=#{length(buffers.video)} audio=#{length(buffers.audio)}")
    after
      Testing.Pipeline.terminate(pipeline)
    end
  end

  defp collect(pipeline, monitor, selected, formats, buffers) do
    if length(Map.get(buffers, :video, [])) >= 25 and length(Map.get(buffers, :audio, [])) >= 100 do
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
              "hang/legacy" = offer.stream_format.packaging

              spec =
                get_child(:catalog)
                |> via_out(Pad.ref(:output, role),
                  options: [track: offer.track_ref, start_policy: :next_group]
                )
                |> child({:adapter, role}, %Legacy{direction: :decode})
                |> child({:sink, role}, Testing.Sink)

              Testing.Pipeline.execute_actions(pipeline, spec: spec)
              Map.put(selected, role, offer.track_ref)
            end

          collect(pipeline, monitor, selected, formats, buffers)

        {Testing.Pipeline, ^pipeline,
         {:handle_child_notification,
          {{:stream_format, :input, %Track{} = format}, {:sink, role}}}} ->
          collect(pipeline, monitor, selected, Map.put(formats, role, format), buffers)

        {Testing.Pipeline, ^pipeline,
         {:handle_child_notification, {{:buffer, %Buffer{} = buffer}, {:sink, role}}}} ->
          collect(
            pipeline,
            monitor,
            selected,
            formats,
            Map.update(buffers, role, [buffer], &[buffer | &1])
          )

        {:DOWN, ^monitor, :process, _pid, reason} ->
          raise "reference receiver failed: #{inspect(reason)}"

        _message ->
          collect(pipeline, monitor, selected, formats, buffers)
      after
        15_000 -> raise "reference legacy media not received"
      end
    end
  end
end

Membrane.MOQX.Interop.LegacyReceiver.run()
