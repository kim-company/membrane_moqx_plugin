defmodule Membrane.MOQX.TrackAdapter.FromTrack do
  @moduledoc """
  Converts the canonical MOQX pad contract into a concrete Membrane format.

  The selected adapter may enrich concrete buffer metadata, but payload bytes
  remain unchanged.
  """

  use Membrane.Filter

  alias Membrane.MOQX.{Track, TrackAdapter}

  def_input_pad :input,
    flow_control: :auto,
    accepted_format: %Track{}

  def_output_pad :output,
    flow_control: :auto,
    accepted_format: _any

  def_options adapter: [spec: module(), required: true],
              adapter_options: [spec: keyword(), default: []]

  @impl true
  def handle_init(_ctx, options) do
    {[],
     %{
       adapter: options.adapter,
       adapter_options: options.adapter_options,
       adapter_state: nil
     }}
  end

  @impl true
  def handle_stream_format(:input, %Track{} = track, _ctx, state) do
    case TrackAdapter.from_moqx_stream_format(state.adapter, track, state.adapter_options) do
      {:ok, stream_format, adapter_state} ->
        {[stream_format: {:output, stream_format}], %{state | adapter_state: adapter_state}}

      {:error, reason} ->
        raise "failed to adapt MOQX stream format: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, %{adapter_state: adapter_state} = state)
      when not is_nil(adapter_state) do
    case TrackAdapter.from_moqx_buffer(state.adapter, buffer, adapter_state) do
      {:ok, buffer, adapter_state} ->
        {[buffer: {:output, buffer}], %{state | adapter_state: adapter_state}}

      {:error, reason} ->
        raise "failed to adapt MOQX buffer: #{inspect(reason)}"
    end
  end
end
