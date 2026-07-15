defmodule Membrane.MOQX.TestDynamicSource do
  @moduledoc false

  use Membrane.Source

  def_output_pad :output,
    availability: :on_request,
    flow_control: :push,
    accepted_format: _any

  def_options stream_format: [spec: struct(), required: true],
              buffer: [spec: Membrane.Buffer.t(), required: true]

  @impl true
  def handle_init(_ctx, options), do: {[], options}

  @impl true
  def handle_playing(ctx, state) do
    actions =
      Enum.flat_map(Map.keys(ctx.pads), fn pad ->
        [stream_format: {pad, state.stream_format}, buffer: {pad, state.buffer}]
      end)

    {actions, state}
  end
end
