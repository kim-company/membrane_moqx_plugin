defmodule Membrane.MOQX.Sink do
  @moduledoc """
  Publishes canonical `Membrane.MOQX.Track` streams through MOQX.

  Each dynamic input pad represents one already-packaged logical track.
  Format-specific filters translate concrete Membrane formats into the stable
  track and unit contract before this element; the Sink owns MOQ coordinates,
  catalog state, and the MOQX client.

  With `inbound_subscriptions: :controlled`, typed MOQX requests are surfaced
  as `{:subscription_requested, request}` parent notifications. The parent
  explicitly accepts or rejects them through child notifications. Approval may
  precede dynamic pad creation; the Sink waits until the named track is
  registered before accepting it through MOQX.

  Subscriber join/leave notifications include the track's current subscriber
  count. Optional `Membrane.MOQX.Event.TrackDemand` events carry only aggregate
  zero/nonzero demand transitions upstream on an established media pad.
  """

  use Membrane.Sink

  alias Membrane.MOQX.{Track, Unit}

  def_input_pad :input,
    availability: :on_request,
    flow_control: :auto,
    accepted_format: %Track{},
    options: [
      track_name: [spec: binary(), required: true],
      init_track_name: [spec: binary() | nil, default: nil],
      retention: [spec: :live | :latest | :all, default: :live]
    ]

  def_options endpoint: [spec: binary() | URI.t(), required: true],
              protocol: [spec: atom() | module(), required: true],
              namespace: [spec: [binary()], required: true],
              authorization: [spec: MOQX.Secret.t() | nil, default: nil],
              timeout: [spec: pos_integer(), default: 5_000],
              connect_options: [spec: keyword(), default: []],
              transport: [spec: term(), default: nil],
              catalog_track_name: [spec: binary(), default: ".catalog"],
              publisher_priority: [spec: 0..255, default: 127],
              inbound_subscriptions: [
                spec: :automatic | :controlled,
                default: :automatic
              ],
              subscription_decision_timeout: [spec: non_neg_integer(), default: 5_000],
              max_pending_subscriptions: [spec: pos_integer(), default: 128],
              infrastructure_subscriptions: [
                spec: :automatic | :controlled,
                default: :automatic
              ],
              track_demand_events: [spec: boolean(), default: false]

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        client: nil,
        publication: nil,
        catalog_track: nil,
        catalog_revision: 0,
        pads: %{},
        pending_subscription_requests: %{},
        joining_subscription_requests: %{},
        active_subscription_requests: %{},
        subscriber_counts: %{}
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    with {:ok, client} <- MOQX.connect(state.endpoint, connect_options(state)),
         {:ok, publication} <- MOQX.publish(client, state.namespace, publication_options(state)) do
      state = %{state | client: client, publication: publication}

      {[setup: :incomplete], state}
    else
      {:error, reason} -> raise "failed to set up MOQX publication: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    case validate_pad_track_names(ctx.pad_options, state) do
      :ok ->
        pad_state = %{
          options: ctx.pad_options,
          track: nil,
          init_track: nil,
          init_track_name: nil,
          media_track: nil,
          generation: 0,
          group_id: 0,
          object_id: 0,
          ended?: false
        }

        {[], put_in(state, [:pads, pad], pad_state)}

      {:error, reason} ->
        notification = {:track_rejected, pad, reason}
        {[notify_parent: notification, terminate: {:shutdown, notification}], state}
    end
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    {pad_state, pads} = Map.pop(state.pads, pad)
    state = %{state | pads: pads}

    cond do
      is_nil(pad_state) or is_nil(pad_state.track) ->
        {[], state}

      pad_state.ended? ->
        {[notify_parent: {:track_removed, pad, pad_state.options.track_name}], state}

      true ->
        case publish_catalog(state) do
          {:ok, state} ->
            actions = [notify_parent: {:track_removed, pad, pad_state.options.track_name}]
            {actions, state}

          {:error, reason} ->
            raise "failed to publish MOQX catalog after pad removal: #{inspect(reason)}"
        end
    end
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    if pad_state.track && !pad_state.ended? do
      case publish_end_of_track(state, pad_state) do
        :ok ->
          finish_track_eos(pad, pad_state, state)

        {:error, reason} ->
          raise "failed to publish MOQX end-of-track status: #{inspect(reason)}"
      end
    else
      {[], state}
    end
  end

  @impl true
  def handle_stream_format(pad, %Track{} = stream_format, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    case Track.validate(stream_format) do
      :ok ->
        cond do
          is_nil(pad_state.track) ->
            prepare_initial_track(pad, stream_format, state)

          stream_format == pad_state.track ->
            {[], state}

          true ->
            prepare_updated_track(pad, stream_format, state)
        end

      {:error, reason} ->
        raise "invalid MOQX track stream format: #{inspect(reason)}"
    end
  end

  defp prepare_initial_track(pad, track, state) do
    pad_state = Map.fetch!(state.pads, pad)
    options = pad_state.options

    with {:ok, init_track, init_name} <-
           prepare_initialization(state, init_track_name(options), track),
         {:ok, media_track} <-
           MOQX.add_track(state.client, state.publication, options.track_name,
             retention: options.retention
           ) do
      pad_state = %{
        pad_state
        | track: track,
          init_track: init_track,
          init_track_name: init_name,
          media_track: media_track
      }

      publish_prepared_track(
        pad,
        pad_state,
        {:track_ready, pad, options.track_name},
        state
      )
    else
      {:error, reason} -> raise "failed to prepare MOQX track: #{inspect(reason)}"
    end
  end

  defp prepare_updated_track(pad, track, state) do
    pad_state = Map.fetch!(state.pads, pad)
    generation = pad_state.generation + 1
    init_name = init_track_name(pad_state.options) <> ".#{generation}"

    case prepare_initialization(state, init_name, track) do
      {:ok, init_track, init_name} ->
        pad_state = %{
          pad_state
          | track: track,
            init_track: init_track,
            init_track_name: init_name,
            generation: generation
        }

        publish_prepared_track(
          pad,
          pad_state,
          {:track_updated, pad, pad_state.options.track_name, generation},
          state
        )

      {:error, reason} ->
        raise "failed to update MOQX track: #{inspect(reason)}"
    end
  end

  defp finish_track_eos(pad, pad_state, state) do
    pad_state = %{pad_state | ended?: true}
    state = put_in(state, [:pads, pad], pad_state)

    case publish_catalog(state) do
      {:ok, state} ->
        actions = [notify_parent: {:track_ended, pad, pad_state.options.track_name}]
        {actions, state}

      {:error, reason} ->
        raise "failed to publish MOQX catalog after end of stream: #{inspect(reason)}"
    end
  end

  defp publish_prepared_track(pad, pad_state, notification, state) do
    state = put_in(state, [:pads, pad], pad_state)

    case publish_catalog(state) do
      {:ok, state} ->
        {subscription_actions, state} =
          accept_approved_subscriptions(
            [pad_state.options.track_name, pad_state.init_track_name],
            state
          )

        {[notify_parent: notification] ++ subscription_actions, state}

      {:error, reason} ->
        raise "failed to publish MOQX catalog: #{inspect(reason)}"
    end
  end

  defp prepare_initialization(_state, _name, %{initialization: nil}) do
    {:ok, nil, nil}
  end

  defp prepare_initialization(state, name, track) do
    with {:ok, init_track} <-
           MOQX.add_track(state.client, state.publication, name, retention: :latest),
         :ok <- publish_initialization(state, init_track, track) do
      {:ok, init_track, name}
    end
  end

  @impl true
  def handle_buffer(pad, buffer, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    with {:ok, unit} <- Unit.from_buffer(buffer),
         :ok <-
           MOQX.publish_object(state.client, pad_state.media_track, %MOQX.Object{
             group_id: pad_state.group_id,
             subgroup_id: 0,
             object_id: pad_state.object_id,
             publisher_priority: state.publisher_priority,
             payload: buffer.payload
           }) do
      pad_state = advance_coordinates(pad_state, unit.group_end?)
      {[], put_in(state, [:pads, pad], pad_state)}
    else
      {:error, reason} -> raise "failed to publish MOQX object: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_parent_notification(
        {:accept_subscription, %MOQX.PublicationSubscriptionRequest{} = request},
        _ctx,
        state
      ) do
    case state.pending_subscription_requests[request.handle] do
      %{request: ^request} = pending ->
        case published_track_for_name(state, request.track.track) do
          nil ->
            pending = %{pending | status: :approved}
            {[], put_in(state, [:pending_subscription_requests, request.handle], pending)}

          published_track ->
            accept_subscription_request(request, published_track, state)
        end

      _other ->
        notification = {:subscription_decision_failed, request, :unknown_subscription_request}
        {[notify_parent: notification], state}
    end
  end

  def handle_parent_notification(
        {:reject_subscription, %MOQX.PublicationSubscriptionRequest{} = request,
         %MOQX.SubscriptionRejection{} = rejection},
        _ctx,
        state
      ) do
    case state.pending_subscription_requests[request.handle] do
      %{request: ^request} ->
        case MOQX.reject_subscription(state.client, request, rejection) do
          :ok ->
            state =
              update_in(state.pending_subscription_requests, &Map.delete(&1, request.handle))

            {[], state}

          {:error, reason} ->
            notification = {:subscription_decision_failed, request, reason}
            {[notify_parent: notification], state}
        end

      _other ->
        notification = {:subscription_decision_failed, request, :unknown_subscription_request}
        {[notify_parent: notification], state}
    end
  end

  def handle_parent_notification(_notification, _ctx, state), do: {[], state}

  @impl true
  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationReady{publication: publication}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    case MOQX.add_track(client, publication, state.catalog_track_name, retention: :latest) do
      {:ok, catalog_track} ->
        state = %{state | catalog_track: catalog_track}

        {subscription_actions, state} =
          accept_approved_subscriptions([state.catalog_track_name], state)

        actions =
          [setup: :complete, notify_parent: {:publication_ready, state.namespace}] ++
            subscription_actions

        {actions, state}

      {:error, reason} ->
        raise "failed to register MOQX catalog track: #{inspect(reason)}"
    end
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationSubscriptionRequested{request: request}},
        _ctx,
        %{client: client} = state
      ) do
    auto_accept? =
      state.infrastructure_subscriptions == :automatic and
        infrastructure_track_name?(state, request.track.track)

    pending = %{request: request, status: if(auto_accept?, do: :approved, else: :pending)}
    state = put_in(state, [:pending_subscription_requests, request.handle], pending)

    if auto_accept? do
      case published_track_for_name(state, request.track.track) do
        nil -> {[], state}
        published_track -> accept_subscription_request(request, published_track, state)
      end
    else
      {[notify_parent: {:subscription_requested, request}], state}
    end
  end

  def handle_info(
        {:moqx, client,
         %MOQX.Event.PublicationSubscriptionCancelled{request: request, reason: reason}},
        _ctx,
        %{client: client} = state
      ) do
    state = update_in(state.pending_subscription_requests, &Map.delete(&1, request.handle))
    {[notify_parent: {:subscription_cancelled, request, reason}], state}
  end

  def handle_info(
        {:moqx, client,
         %MOQX.Event.PublicationSubscriberJoined{track: track, request_id: request_id}},
        _ctx,
        %{client: client} = state
      ) do
    track_name = published_track_name(track)
    {identity, state} = take_joining_subscription(state, track_name, request_id)
    previous_count = Map.get(state.subscriber_counts, track_name, 0)
    count = previous_count + 1

    state =
      state
      |> put_in([:active_subscription_requests, request_id], identity)
      |> put_in([:subscriber_counts, track_name], count)

    notification = {:subscriber_joined, track_name, identity, count}

    actions =
      [notify_parent: notification] ++
        track_demand_actions(state, track_name, previous_count, count)

    {actions, state}
  end

  def handle_info(
        {:moqx, client,
         %MOQX.Event.PublicationSubscriberLeft{track: track, request_id: request_id}},
        _ctx,
        %{client: client} = state
      ) do
    track_name = published_track_name(track)
    {identity, active} = Map.pop(state.active_subscription_requests, request_id, request_id)
    previous_count = Map.get(state.subscriber_counts, track_name, 1)
    count = max(previous_count - 1, 0)

    state = %{
      state
      | active_subscription_requests: active,
        subscriber_counts: Map.put(state.subscriber_counts, track_name, count)
    }

    notification = {:subscriber_left, track_name, identity, count}

    actions =
      [notify_parent: notification] ++
        track_demand_actions(state, track_name, previous_count, count)

    {actions, state}
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationFailed{publication: publication, error: error}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    terminate_after_event({:publication_failed, error}, state, :close)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationCancelled{publication: publication, error: error}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    terminate_after_event({:publication_cancelled, error}, state, :close)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.ConnectionClosed{metadata: metadata}},
        _ctx,
        %{client: client} = state
      ) do
    terminate_after_event({:connection_closed, metadata}, state, :already_closed)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.ProtocolFailed{reason: reason}},
        _ctx,
        %{client: client} = state
      ) do
    terminate_after_event({:protocol_failed, reason}, state, :already_closed)
  end

  def handle_info(_message, _ctx, state), do: {[], state}

  @impl true
  def handle_terminate_request(_ctx, state) do
    finish_publication(state)
    close_client(state)

    {[terminate: :normal], state}
  end

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

  defp publication_options(%{inbound_subscriptions: :automatic}), do: []

  defp publication_options(state) do
    [
      inbound_subscriptions: :controlled,
      subscription_decision_timeout: state.subscription_decision_timeout,
      max_pending_subscriptions: state.max_pending_subscriptions
    ]
  end

  defp put_if_present(options, _key, nil), do: options
  defp put_if_present(options, key, value), do: Keyword.put(options, key, value)

  defp published_track_name(track) do
    track
    |> MOQX.PublishedTrack.track_ref()
    |> Map.fetch!(:track)
  end

  defp published_track_for_name(state, track_name) do
    if state.catalog_track && published_track_name(state.catalog_track) == track_name do
      state.catalog_track
    else
      Enum.find_value(state.pads, fn {_pad, pad_state} ->
        pad_published_track_for_name(pad_state, track_name)
      end)
    end
  end

  defp pad_published_track_for_name(pad_state, track_name) do
    Enum.find([pad_state.media_track, pad_state.init_track], fn
      nil -> false
      track -> published_track_name(track) == track_name
    end)
  end

  defp accept_subscription_request(request, published_track, state) do
    case MOQX.accept_subscription(state.client, request, published_track) do
      :ok ->
        track_name = request.track.track

        state =
          state
          |> update_in([:pending_subscription_requests], &Map.delete(&1, request.handle))
          |> update_in([:joining_subscription_requests, track_name], fn
            nil -> [request]
            requests -> requests ++ [request]
          end)

        {[], state}

      {:error, reason} ->
        notification = {:subscription_decision_failed, request, reason}
        {[notify_parent: notification], state}
    end
  end

  defp accept_approved_subscriptions(track_names, state) do
    track_names = MapSet.new(track_names -- [nil])

    state.pending_subscription_requests
    |> Enum.filter(fn {_handle, pending} ->
      pending.status == :approved and MapSet.member?(track_names, pending.request.track.track)
    end)
    |> Enum.reduce({[], state}, fn {_handle, pending}, {actions, state} ->
      published_track = published_track_for_name(state, pending.request.track.track)

      if published_track do
        {next_actions, state} =
          accept_subscription_request(pending.request, published_track, state)

        {actions ++ next_actions, state}
      else
        {actions, state}
      end
    end)
  end

  defp take_joining_subscription(state, track_name, request_id) do
    case Map.get(state.joining_subscription_requests, track_name, []) do
      [request | rest] ->
        joining =
          if rest == [] do
            Map.delete(state.joining_subscription_requests, track_name)
          else
            Map.put(state.joining_subscription_requests, track_name, rest)
          end

        {request.handle, %{state | joining_subscription_requests: joining}}

      [] ->
        {request_id, state}
    end
  end

  defp track_demand_actions(%{track_demand_events: false}, _track_name, _previous, _count),
    do: []

  defp track_demand_actions(state, track_name, previous_count, count)
       when (previous_count == 0 and count == 1) or (previous_count == 1 and count == 0) do
    case pad_for_track_name(state, track_name) do
      nil ->
        []

      pad ->
        event = %Membrane.MOQX.Event.TrackDemand{
          subscriber_count: count,
          active?: count > 0
        }

        [event: {pad, event}]
    end
  end

  defp track_demand_actions(_state, _track_name, _previous_count, _count), do: []

  defp pad_for_track_name(state, track_name) do
    Enum.find_value(state.pads, fn {pad, pad_state} ->
      if pad_state.options.track_name == track_name, do: pad
    end)
  end

  defp infrastructure_track_name?(state, track_name) do
    track_name == state.catalog_track_name or
      Enum.any?(state.pads, fn {_pad, pad_state} ->
        init_track_name(pad_state.options) == track_name
      end)
  end

  defp init_track_name(%{init_track_name: nil, track_name: track_name}),
    do: track_name <> ".init"

  defp init_track_name(%{init_track_name: name}), do: name

  defp validate_pad_track_names(options, state) do
    track_name = options.track_name
    init_name = init_track_name(options)

    media_names = Enum.map(state.pads, fn {_pad, pad_state} -> pad_state.options.track_name end)

    init_names =
      Enum.map(state.pads, fn {_pad, pad_state} -> init_track_name(pad_state.options) end)

    cond do
      track_name == state.catalog_track_name ->
        {:error, {:reserved_track_name, track_name}}

      track_name in media_names ->
        {:error, {:duplicate_track_name, track_name}}

      track_name in init_names ->
        {:error, {:track_name_conflicts_with_init_track, track_name}}

      init_name == track_name or init_name == state.catalog_track_name or init_name in media_names ->
        {:error, {:init_track_name_conflict, init_name}}

      init_name in init_names ->
        {:error, {:duplicate_init_track_name, init_name}}

      true ->
        :ok
    end
  end

  defp publish_initialization(state, init_track, track) do
    MOQX.publish_object(state.client, init_track, %MOQX.Object{
      group_id: 0,
      subgroup_id: 0,
      object_id: 0,
      publisher_priority: state.publisher_priority,
      payload: track.initialization
    })
  end

  defp publish_end_of_track(state, pad_state) do
    MOQX.publish_object(state.client, pad_state.media_track, %MOQX.Object{
      group_id: pad_state.group_id,
      subgroup_id: 0,
      object_id: pad_state.object_id,
      publisher_priority: state.publisher_priority,
      status: :end_of_track,
      payload: <<>>
    })
  end

  defp publish_catalog(state) do
    payload =
      JSON.encode!(%{
        "version" => 1,
        "streamingFormat" => 1,
        "streamingFormatVersion" => "0.2",
        "supportsDeltaUpdates" => false,
        "commonTrackFields" => %{
          "namespace" => Enum.join(state.namespace, "/")
        },
        "tracks" => catalog_tracks(state)
      })

    object = %MOQX.Object{
      group_id: state.catalog_revision,
      subgroup_id: 0,
      object_id: 0,
      publisher_priority: state.publisher_priority,
      payload: payload
    }

    case MOQX.publish_object(state.client, state.catalog_track, object) do
      :ok -> {:ok, %{state | catalog_revision: state.catalog_revision + 1}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp catalog_tracks(state) do
    state.pads
    |> Enum.filter(fn {_pad, pad_state} ->
      not is_nil(pad_state.track) and !pad_state.ended?
    end)
    |> Enum.sort_by(fn {_pad, pad_state} -> pad_state.options.track_name end)
    |> Enum.map(fn {_pad, pad_state} -> catalog_track(pad_state) end)
  end

  defp catalog_track(pad_state) do
    options = pad_state.options
    track = pad_state.track

    track.catalog_fields
    |> Map.put("name", options.track_name)
    |> Map.put("packaging", track.packaging)
    |> put_map_unless_empty("selectionParams", track.selection_params)
    |> put_map_if_present("initTrack", pad_state.init_track_name)
  end

  defp put_map_if_present(map, _key, nil), do: map
  defp put_map_if_present(map, key, value), do: Map.put(map, key, value)

  defp put_map_unless_empty(map, _key, value) when value == %{}, do: map
  defp put_map_unless_empty(map, key, value), do: Map.put(map, key, value)

  defp advance_coordinates(pad_state, true) do
    %{pad_state | group_id: pad_state.group_id + 1, object_id: 0}
  end

  defp advance_coordinates(pad_state, false) do
    %{pad_state | object_id: pad_state.object_id + 1}
  end

  defp finish_publication(%{client: nil}), do: :ok
  defp finish_publication(%{publication: nil}), do: :ok

  defp finish_publication(state) do
    case MOQX.finish_publication(state.client, state.publication) do
      :ok ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to finish MOQX publication: #{inspect(reason)}")
    end
  end

  defp close_client(%{client: nil}), do: :ok

  defp close_client(state) do
    case MOQX.close(state.client) do
      :ok ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to close MOQX client: #{inspect(reason)}")
    end
  end

  defp terminate_after_event(notification, state, :close) do
    close_client(state)
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  defp terminate_after_event(notification, state, :already_closed) do
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end
end
