defmodule Membrane.MOQX.Source do
  @moduledoc """
  Subscribes to exactly one MOQ track and emits the canonical MOQX pad format.

  Payload bytes remain unchanged. Received MOQ coordinates, priority, status,
  and inferred group boundaries are stored in Membrane.MOQX.Unit metadata.
  Objects preserve order within each subgroup. Interleaved subgroup streams
  become eligible independently when their own subgroup advances or completes.

  `protocol` is explicit and never inferred from the endpoint. A Source using
  a shared `Membrane.MOQX.Session` must select the same protocol as the Session;
  setup fails before subscribing when they differ.
  `subscription_options` pass through to `MOQX.subscribe/3`, including
  protocol-neutral start/filter, priority, group order, delivery timeout, and
  extension parameters supported by the selected MOQX implementation.
  The parent can send `{:update_subscription, request_id, options}` at runtime.
  `{:subscription_update_result, request_id, result}` reports local transport
  admission only. Actual peer responses independently report
  `{:subscription_updated, track_ref, parameters}` or
  `{:subscription_update_failed, track_ref, error}`; no command correlation is
  inferred. Lite has no update acknowledgement. An invalid/stale update or a
  draft-16 peer rejection does not by itself terminate the active Source.
  Shared sessions enforce the calling Source's ownership of its subscription.

  For `:moq_lite_05`, the Source converts each frame timestamp from the
  immutable track timescale into Membrane nanoseconds. The timestamp is
  independent of group and object coordinates; payload bytes and group
  boundaries remain unchanged.

  The caller supplies the exact track and its canonical stream format. This
  element does not discover broadcasts, parse HANG catalogs, infer codecs,
  demux media, or pace playback. Use `Membrane.MOQX.CatalogSource` with an
  explicit HANG or CMSF profile for catalog offers. Complete zero-object Lite
  groups emit `Membrane.MOQX.Event.EmptyGroup`, preserving their group ID, not
  a buffer or EOS. HANG interprets this event as a codec-epoch boundary;
  downstream decoders own resetting and playback policy. Partial/reset groups
  and zero-byte objects are not empty-group boundaries. Events follow receive
  order; this Source does not globally reorder concurrent group streams.

  Output uses push flow control. Subscriber demand at a remote publisher is
  not downstream Membrane demand. Completion describes the received protocol
  lifecycle, not proof that every intended publisher buffer arrived; see
  `Membrane.MOQX` for the observed relay completion limitations.

      child(:source, %Membrane.MOQX.Source{
        endpoint: "moql://cdn.moq.dev:443",
        protocol: :moq_lite_05,
        track: %MOQX.TrackRef{namespace: ["speech"], track: "opus"},
        stream_format: %Membrane.MOQX.Track{packaging: "opus", initialization: nil}
      })
  """

  use Membrane.Source

  alias Membrane.MOQX.{ProtocolConventions, Session, Timestamp, Track, Unit}
  alias MOQX.Protocol.Resolver

  def_output_pad :output,
    flow_control: :push,
    accepted_format: %Track{}

  def_options endpoint: [spec: binary() | URI.t() | nil, default: nil],
              protocol: [spec: atom() | module(), required: true],
              session: [spec: pid() | nil, default: nil],
              track: [spec: MOQX.TrackRef.t(), required: true],
              stream_format: [spec: Track.t(), required: true],
              authorization: [spec: MOQX.Secret.t() | nil, default: nil],
              timeout: [spec: pos_integer(), default: 5_000],
              connect_options: [spec: keyword(), default: []],
              transport: [spec: term(), default: nil],
              start_policy: [spec: :current | :next_group, default: :current],
              subscription_options: [spec: keyword(), default: []]

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        client: nil,
        subscription: nil,
        accepted?: false,
        playing?: false,
        pending_objects: %{},
        queued_events: [],
        initial_group: nil,
        track_timescale: nil,
        track_publisher_priority: nil,
        ended?: false
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    with :ok <- Track.validate(state.stream_format),
         :ok <- validate_connection_options(state),
         {:ok, client, subscription} <- start_subscription(state) do
      state = %{state | client: client, subscription: subscription}
      {[setup: :incomplete], state}
    else
      {:error, reason} -> raise "failed to set up MOQX subscription: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_playing(_ctx, state) do
    state = %{state | playing?: true}
    {queued_actions, state} = consume_queued_events(state)
    {[stream_format: {:output, state.stream_format}] ++ queued_actions, state}
  end

  @impl true
  def handle_info({:moqx, client, event}, ctx, %{client: client} = state),
    do: handle_event(event, ctx, state)

  def handle_info({:moqx_session, session, event}, ctx, %{session: session} = state),
    do: handle_event(event, ctx, state)

  def handle_info(_message, _ctx, state), do: {[], state}

  @impl true
  def handle_parent_notification({:update_subscription, request_id, options}, _ctx, state) do
    result =
      cond do
        state.ended? or is_nil(state.subscription) ->
          {:error, :unknown_subscription}

        not state.accepted? ->
          {:error, :subscription_not_ready}

        is_pid(state.session) ->
          Session.update_subscription(state.session, state.subscription, options)

        true ->
          MOQX.update_subscription(state.client, state.subscription, options)
      end

    {[notify_parent: {:subscription_update_result, request_id, result}], state}
  end

  def handle_parent_notification(_notification, _ctx, state), do: {[], state}

  defp handle_event(
         %MOQX.Event.SubscriptionAccepted{
           subscription: subscription,
           track_info: track_info
         },
         _ctx,
         %{subscription: subscription} = state
       ) do
    actions = [
      setup: :complete,
      notify_parent: {:subscription_ready, state.track}
    ]

    timescale = if track_info, do: track_info.timescale
    publisher_priority = if track_info, do: track_info.publisher_priority

    {actions,
     %{
       state
       | accepted?: true,
         track_timescale: timescale,
         track_publisher_priority: publisher_priority
     }}
  end

  defp handle_event(
         %MOQX.Event.ObjectReceived{object: object},
         _ctx,
         %{subscription: subscription} = state
       )
       when object.subscription == subscription do
    consume_or_queue({:object, object}, state)
  end

  defp handle_event(
         %MOQX.Event.ObjectStatus{object: object},
         _ctx,
         %{subscription: subscription} = state
       )
       when object.subscription == subscription do
    consume_or_queue({:object, object}, state)
  end

  defp handle_event(
         %MOQX.Event.SubscriptionDone{
           subscription: subscription,
           completion: completion
         },
         _ctx,
         %{subscription: subscription} = state
       ) do
    consume_or_queue({:subscription_done, completion}, state)
  end

  defp handle_event(
         %MOQX.Event.SubgroupEnded{subscription: subscription} = event,
         _ctx,
         %{subscription: subscription} = state
       ) do
    consume_or_queue({:subgroup_ended, event}, state)
  end

  defp handle_event(
         %MOQX.Event.SubscriptionUpdated{subscription: subscription, parameters: parameters},
         _ctx,
         %{subscription: subscription} = state
       ) do
    {[notify_parent: {:subscription_updated, state.track, parameters}], state}
  end

  defp handle_event(
         %MOQX.Event.SubscriptionUpdateFailed{subscription: subscription, error: error},
         _ctx,
         %{subscription: subscription} = state
       ) do
    {[notify_parent: {:subscription_update_failed, state.track, error}], state}
  end

  defp handle_event(
         %MOQX.Event.SubscriptionFailed{subscription: subscription, error: error},
         _ctx,
         %{subscription: subscription} = state
       ) do
    notification = {:subscription_failed, state.track, error}

    actions =
      if(state.accepted?, do: [], else: [setup: :complete]) ++
        [notify_parent: notification, terminate: {:shutdown, notification}]

    {actions, state}
  end

  defp handle_event(%MOQX.Event.ConnectionClosed{metadata: metadata}, _ctx, state) do
    notification = {:connection_closed, metadata}
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  defp handle_event(%MOQX.Event.ProtocolFailed{reason: reason}, _ctx, state) do
    notification = {:protocol_failed, reason}
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  defp handle_event(_event, _ctx, state), do: {[], state}

  @impl true
  def handle_terminate_request(_ctx, state) do
    unsubscribe(state)
    close_client(state)
    {[terminate: :normal], state}
  end

  defp consume_or_queue(event, %{playing?: false} = state) do
    {[], update_in(state.queued_events, &(&1 ++ [event]))}
  end

  defp consume_or_queue(event, state), do: consume_event(event, state)

  defp consume_queued_events(state) do
    events = state.queued_events
    state = %{state | queued_events: []}

    Enum.reduce(events, {[], state}, fn event, {actions, state} ->
      {next_actions, state} = consume_event(event, state)
      {actions ++ next_actions, state}
    end)
  end

  defp consume_event({:object, object}, state) do
    key = object_subgroup_key(object, state)

    case Map.pop(state.pending_objects, key) do
      {nil, _pending_objects} ->
        case start_object?(object, state) do
          {true, state} -> {[], put_in(state, [:pending_objects, key], object)}
          {false, state} -> {[], state}
        end

      {previous, pending_objects} ->
        group_end? = object_ends_group?(previous, object)
        action = {:buffer, {:output, object_buffer(previous, group_end?, state)}}
        pending_objects = Map.put(pending_objects, key, object)
        {[action], %{state | pending_objects: pending_objects}}
    end
  end

  defp consume_event({:subscription_done, completion}, %{ended?: false} = state) do
    buffer_actions =
      state.pending_objects
      |> Enum.sort_by(fn {_key, object} ->
        {object.group_id, object.subgroup_id, object.object_id}
      end)
      |> Enum.map(fn {_key, object} ->
        {:buffer, {:output, object_buffer(object, true, state)}}
      end)

    notification = {:subscription_done, state.track, completion}

    actions =
      buffer_actions ++
        [
          notify_parent: notification,
          end_of_stream: :output
        ]

    {actions, %{state | pending_objects: %{}, ended?: true}}
  end

  defp consume_event({:subscription_done, _completion}, state), do: {[], state}

  defp consume_event(
         {:subgroup_ended,
          %MOQX.Event.SubgroupEnded{object_count: 0, outcome: :complete, end_of_group?: true} =
            event},
         %{protocol: protocol} = state
       )
       when protocol in [:moq_lite_05, MOQX.Protocol.MOQLite05] do
    case start_object?(event, state) do
      {true, state} ->
        {[event: {:output, %Membrane.MOQX.Event.EmptyGroup{group_id: event.group_id}}], state}

      {false, state} ->
        {[], state}
    end
  end

  defp consume_event(
         {:subgroup_ended, event},
         state
       ) do
    key = {event.group_id, event.subgroup_id}

    case Map.pop(state.pending_objects, key) do
      {nil, _pending_objects} ->
        {[], state}

      {object, pending_objects} ->
        complete? = event.outcome == :complete and event.end_of_group?
        actions = [buffer: {:output, object_buffer(object, complete?, state)}]
        {actions, %{state | pending_objects: pending_objects}}
    end
  end

  defp object_subgroup_key(%{group_id: group_id, subgroup_id: nil}, state) do
    if ProtocolConventions.draft_16?(state.protocol),
      do: :draft_16_datagram,
      else: {group_id, nil}
  end

  defp object_subgroup_key(object, _state), do: {object.group_id, object.subgroup_id}

  defp object_ends_group?(previous, next) do
    previous.group_id != next.group_id or previous.end_of_group? == true or
      previous.status in [:end_of_group, :end_of_track]
  end

  defp object_buffer(object, group_end?, state) do
    unit = %Unit{
      group_end?: group_end?,
      group_id: object.group_id,
      subgroup_id: object.subgroup_id,
      object_id: object.object_id,
      publisher_priority: object.publisher_priority || state.track_publisher_priority,
      status: object.status
    }

    %Membrane.Buffer{
      payload: object.payload,
      pts: object_pts(object, state.track_timescale),
      metadata: %{moqx: unit}
    }
  end

  defp object_pts(%{timestamp: nil}, _timescale), do: nil
  defp object_pts(_object, nil), do: nil

  defp object_pts(object, timescale) do
    case Timestamp.to_membrane_time(object.timestamp, timescale) do
      {:ok, pts} -> pts
      {:error, reason} -> raise "invalid MOQX object timestamp: #{inspect(reason)}"
    end
  end

  defp start_object?(_object, %{start_policy: :current} = state), do: {true, state}

  defp start_object?(object, %{start_policy: :next_group, initial_group: nil} = state),
    do: {false, %{state | initial_group: object.group_id}}

  defp start_object?(object, %{start_policy: :next_group, initial_group: group} = state),
    do: {object.group_id != group, state}

  defp connect_options(state) do
    [
      protocol: state.protocol,
      events_to: self(),
      timeout: state.timeout,
      connect_options: state.connect_options
    ]
    |> put_if_present(:authorization, state.authorization)
    |> put_if_present(:transport, state.transport)
  end

  defp put_if_present(options, _key, nil), do: options
  defp put_if_present(options, key, value), do: Keyword.put(options, key, value)

  defp validate_connection_options(%{session: session, protocol: protocol})
       when is_pid(session) and not is_nil(protocol) do
    session_protocol = Session.protocol(session)

    with {:ok, source_module} <- Resolver.fetch(protocol),
         {:ok, session_module} <- Resolver.fetch(session_protocol),
         true <- source_module == session_module do
      :ok
    else
      _mismatch ->
        {:error, {:session_protocol_mismatch, %{source: protocol, session: session_protocol}}}
    end
  end

  defp validate_connection_options(%{endpoint: endpoint, protocol: protocol})
       when (is_binary(endpoint) or is_struct(endpoint, URI)) and not is_nil(protocol),
       do: :ok

  defp validate_connection_options(_state),
    do: {:error, :source_requires_session_or_endpoint_and_protocol}

  defp start_subscription(%{session: session} = state) when is_pid(session) do
    case Session.subscribe(session, state.track, state.subscription_options) do
      {:ok, subscription} -> {:ok, nil, subscription}
      {:error, _reason} = error -> error
    end
  end

  defp start_subscription(state) do
    with {:ok, client} <- MOQX.connect(state.endpoint, connect_options(state)),
         {:ok, subscription} <- MOQX.subscribe(client, state.track, state.subscription_options) do
      {:ok, client, subscription}
    end
  end

  defp unsubscribe(%{subscription: nil}), do: :ok

  defp unsubscribe(%{session: session} = state) when is_pid(session) do
    case Session.unsubscribe(session, state.subscription) do
      :ok ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to unsubscribe shared MOQX Source: #{inspect(reason)}")
    end
  catch
    :exit, {:noproc, _call} -> :ok
    :exit, {:normal, _call} -> :ok
  end

  defp unsubscribe(%{client: nil}), do: :ok

  defp unsubscribe(state) do
    case MOQX.unsubscribe(state.client, state.subscription) do
      :ok ->
        :ok

      {:error, :unknown_subscription} ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to unsubscribe MOQX Source: #{inspect(reason)}")
    end
  end

  defp close_client(%{session: session}) when is_pid(session), do: :ok
  defp close_client(%{client: nil}), do: :ok

  defp close_client(state) do
    case MOQX.close(state.client) do
      :ok ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to close MOQX Source: #{inspect(reason)}")
    end
  end
end
