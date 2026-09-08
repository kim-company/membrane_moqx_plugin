defmodule Membrane.MOQX.Hang.Legacy do
  @moduledoc """
  Explicit HANG legacy-container framing for already-encoded canonical media.

  With `direction: :encode`, an Opus or H.264 `Track` becomes `hang/legacy`; each buffer
  gets one QUIC-varint timestamp prefix in microseconds. Its Membrane PTS and
  Unit metadata remain available to the Sink for independent Lite transport
  framing. This filter is not a codec encoder, decoder, or playback clock.

  With `direction: :decode`, the prefix replaces the transport-derived PTS;
  timestamps are never added together. Codec payloads and decoder initialization
  are preserved. H.264 input buffers must contain complete access units and set
  `metadata.keyframe?`: each group starts with a keyframe and subsequent frames
  are delta frames. This filter trusts that metadata; it does not parse NAL units.
  Decoding restores `keyframe?` from the received object index (zero starts a
  group), or from group boundaries when coordinates are absent. Received groups
  may interleave; the flag does not imply that this adapter reorders playback.
  A decoder/player consuming multiple groups must provide its own ordering and
  playback policy; global arrival-order PTS is not guaranteed by this filter.

  A decoded empty codec payload emits `Membrane.MOQX.Event.MediaEnd`, not EOS.
  Flush packets may follow that exclusive media endpoint. Downstream decoders
  must apply the endpoint to decoded samples; encoded packets cannot be trimmed
  here. Encoding that event restores the empty-payload container frame.

  Empty-group codec-epoch discontinuities are not supported by the current
  Source/Sink boundary (upstream MOQX #48). They are distinct from MediaEnd:
  a discontinuity contains no frame, while MediaEnd is a timestamped frame.
  This adapter does not reset a decoder or make backward-PTS epochs publishable.

  The container format is pinned by MOQX 0.9.0's HANG reference at moq-dev/moq
  `fd477082c43c3c0738fb62d077d85ea078f10045`. Both the container prefix and the
  Lite transport frame carry timing; neither is a substitute for the other.
  """
  use Membrane.Filter
  alias Membrane.MOQX.Event.MediaEnd
  alias Membrane.MOQX.{Timestamp, Track, Unit}

  def_input_pad :input, flow_control: :auto, accepted_format: %Track{}
  def_output_pad :output, flow_control: :auto, accepted_format: %Track{}
  def_options direction: [spec: :encode | :decode, required: true]

  @impl true
  def handle_init(_ctx, options),
    do: {[], %{direction: options.direction, format: nil, group_open?: false}}

  @impl true
  def handle_stream_format(
        :input,
        %Track{packaging: packaging} = format,
        _ctx,
        %{direction: :encode} = state
      )
      when packaging in ["opus", "h264"] do
    role = if packaging == "h264", do: "video", else: "audio"

    encoded = %{
      format
      | packaging: "hang/legacy",
        catalog_fields: Map.put(format.catalog_fields, "role", role)
    }

    {[stream_format: {:output, encoded}], %{state | format: encoded}}
  end

  def handle_stream_format(
        :input,
        %Track{packaging: "hang/legacy", selection_params: %{"codec" => codec}} = format,
        _ctx,
        %{direction: :decode} = state
      ) do
    packaging =
      cond do
        codec == "opus" ->
          "opus"

        is_binary(codec) and
            (String.starts_with?(codec, "avc1.") or String.starts_with?(codec, "avc3.")) ->
          "h264"

        true ->
          raise ArgumentError, "unsupported HANG legacy codec"
      end

    decoded = %{format | packaging: packaging}
    {[stream_format: {:output, decoded}], %{state | format: decoded}}
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, %{direction: :encode} = state) do
    with {:ok, unit} <- Unit.from_buffer(buffer),
         :ok <- validate_group(buffer, state),
         {:ok, timestamp} <- Timestamp.from_membrane_time(buffer.pts, 1_000_000),
         true <- timestamp < 4_611_686_018_427_387_904 do
      payload = MOQX.Codec.encode_varint(timestamp) <> buffer.payload

      {[buffer: {:output, %{buffer | payload: payload}}],
       %{state | group_open?: not unit.group_end?}}
    else
      reason -> raise ArgumentError, "invalid HANG legacy buffer: #{inspect(reason)}"
    end
  end

  def handle_buffer(:input, buffer, _ctx, %{direction: :decode} = state) do
    with {:ok, unit} <- Unit.from_buffer(buffer),
         {:ok, timestamp, payload} <- MOQX.Codec.decode_varint(buffer.payload) do
      metadata =
        if state.format.packaging == "h264",
          do: Map.put(buffer.metadata, :keyframe?, keyframe?(unit, state)),
          else: buffer.metadata

      actions =
        case payload do
          <<>> ->
            [event: {:output, %MediaEnd{pts: timestamp * 1_000, unit: unit}}]

          _ ->
            [
              buffer:
                {:output,
                 %{buffer | payload: payload, pts: timestamp * 1_000, metadata: metadata}}
            ]
        end

      {actions, %{state | group_open?: not unit.group_end?}}
    else
      reason -> raise ArgumentError, "invalid HANG legacy frame: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_event(:input, %MediaEnd{pts: pts, unit: unit}, ctx, %{direction: :encode} = state) do
    handle_buffer(
      :input,
      %Membrane.Buffer{payload: <<>>, pts: pts, metadata: %{moqx: unit}},
      ctx,
      state
    )
  end

  def handle_event(pad, event, ctx, state), do: super(pad, event, ctx, state)

  defp keyframe?(%Unit{object_id: id}, _state) when is_integer(id), do: id == 0
  defp keyframe?(_unit, state), do: not state.group_open?

  defp validate_group(%{payload: <<>>}, _state), do: :ok

  defp validate_group(buffer, %{format: %{catalog_fields: %{"role" => "video"}}} = state) do
    case {state.group_open?, buffer.metadata[:keyframe?]} do
      {false, true} -> :ok
      {true, false} -> :ok
      {false, _} -> {:error, :video_group_must_start_with_keyframe}
      {true, _} -> {:error, :video_group_requires_delta_frame}
    end
  end

  defp validate_group(_buffer, _state), do: :ok
end
