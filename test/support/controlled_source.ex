defmodule Membrane.MOQX.TestControlledSource do
  @moduledoc false

  use Membrane.Source

  def_output_pad :output,
    flow_control: :push,
    accepted_format: _any

  def_options stream_format: [spec: struct(), required: true]

  @impl true
  def handle_init(_ctx, options), do: {[], options}

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, state.stream_format}], state}
  end

  @impl true
  def handle_parent_notification({:publish, buffers}, _ctx, state) do
    actions = Enum.map(buffers, &{:buffer, {:output, &1}})
    {actions, state}
  end

  def handle_parent_notification(:end_of_stream, _ctx, state) do
    {[end_of_stream: :output], state}
  end

  def handle_parent_notification({:stream_format, format}, _ctx, state) do
    {[stream_format: {:output, format}], %{state | stream_format: format}}
  end

  def handle_parent_notification({:event, event}, _ctx, state) do
    {[event: {:output, event}], state}
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
