defmodule Membrane.MOQX.TrackAdapter.ToTrack do
  @moduledoc """
  Converts a concrete Membrane stream format into the canonical MOQX contract.

  The selected adapter normalizes stream formats and buffers. Payload bytes are
  never modified.
  """

  use Membrane.Filter

  alias Membrane.MOQX.{Track, TrackAdapter}

  def_input_pad :input,
    flow_control: :auto,
    accepted_format: _any

  def_output_pad :output,
    availability: :on_request,
    flow_control: :auto,
    accepted_format: %Track{}

  def_options adapter: [spec: module(), required: true],
              adapter_options: [spec: keyword(), default: []]

  @impl true
  def handle_init(_ctx, options) do
    {[],
     %{adapter: options.adapter, adapter_options: options.adapter_options, adapter_state: nil}}
  end

  @impl true
  def handle_stream_format(:input, stream_format, _ctx, state) do
    case TrackAdapter.to_moqx_stream_format(state.adapter, stream_format, state.adapter_options) do
      {:ok, track, adapter_state} ->
        {[broadcast: track], %{state | adapter_state: adapter_state}}

      {:error, reason} ->
        raise "failed to adapt stream format to MOQX: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, %{adapter_state: adapter_state} = state)
      when not is_nil(adapter_state) do
    case TrackAdapter.to_moqx_buffer(state.adapter, buffer, adapter_state) do
      {:ok, buffer, adapter_state} ->
        {[broadcast: buffer], %{state | adapter_state: adapter_state}}

      {:error, reason} ->
        raise "failed to adapt buffer to MOQX: #{inspect(reason)}"
    end
  end
end
