defmodule Membrane.MOQX.TestDynamicSource do
  @moduledoc false

  use Membrane.Source

  def_output_pad :output,
    availability: :on_request,
    flow_control: :push,
    accepted_format: _any

  def_options stream_format: [spec: struct(), required: true],
              buffer: [spec: Membrane.Buffer.t() | nil, default: nil]

  @impl true
  def handle_init(_ctx, options), do: {[], options}

  @impl true
  def handle_playing(ctx, state) do
    actions =
      Enum.flat_map(Map.keys(ctx.pads), fn pad ->
        [stream_format: {pad, state.stream_format}] ++
          if(state.buffer, do: [buffer: {pad, state.buffer}], else: [])
      end)

    {actions, state}
  end

  @impl true
  def handle_parent_notification({:publish, pad, buffers}, _ctx, state) do
    {[buffer: {pad, buffers}], state}
  end

  @impl true
  def handle_event(
        pad,
        %{__struct__: Membrane.MOQX.Event.TrackDemand} = event,
        _ctx,
        state
      ) do
    {[notify_parent: {:track_demand, pad, event}], state}
  end
end
