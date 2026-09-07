defmodule Membrane.MOQX.Sink do
  @moduledoc """
  Publishes canonical `Membrane.MOQX.Track` streams through MOQX.

  Each dynamic input pad represents one already-packaged logical track.
  Format-specific filters translate concrete Membrane formats into the stable
  track and unit contract before this element; the Sink owns MOQ coordinates,
  catalog state, and the MOQX client.

  `profile` is selected independently of the wire protocol. The default
  `:none` publishes raw tracks without a catalog. Select `:cloudflare_cmsf`,
  `:moqtail_cmsf`, or `:hang` explicitly for catalog publication. HANG requires
  Lite05 and publishes retained `catalog.json` snapshots through MOQX's catalog
  API. A new HANG publication starts with an empty catalog; dynamic media tracks
  replace that snapshot. Media packaging must match the selected profile.
  Use `Membrane.MOQX.Hang.Legacy` for Opus/H.264 legacy framing, or
  `Membrane.MOQX.Hang.CMAF` with `TrackAdapter.ToTrack` for H.264/AAC CMAF.
  These are explicit adapters; this Sink does not encode media or add framing.

  With `:none`, canonical initialization remains caller-owned metadata; no
  catalog or initialization track is synthesized. Current CMSF publication
  verification covers Cloudflare CMSF on draft-14 and Moqtail CMSF on draft-16.
  Although MOQX accepts other CMSF/transport compositions, this Sink's
  initialization lifecycle has not yet been made profile-owned throughout:
  do not use those cross-compositions for initialized media (PR #12 review).

  With `inbound_subscriptions: :controlled`, typed MOQX requests are surfaced
  as `{:subscription_requested, request}` parent notifications. The parent
  explicitly accepts or rejects them through child notifications. Approval may
  precede dynamic pad creation; the Sink waits until the named track is
  registered before accepting it through MOQX.
  This is not a guarantee of absent-track provisioning through every relay:
  the pinned moq-dev Lite05 relay requests TrackInfo before admission, and
  MOQX 0.9.0 rejects TrackInfo for an unregistered track. With that relay,
  register the track first; controlled admission still applies afterward.

  Subscriber join/leave notifications include the track's current subscriber
  count on this MOQX connection, not the viewer count behind a relay. A relay
  may aggregate multiple downstream Sources into a single upstream subscription.
  Optional `Membrane.MOQX.Event.TrackDemand` events carry only aggregate
  zero/nonzero demand transitions upstream on an established media pad. Pad
  removal terminates that event edge and does not synthesize a final inactive
  event on the detached pad; the parent still observes subscriber departure.
  A parent can finish one accepted subscriber without withdrawing the track by
  sending `{:finish_subscription, request_handle, options}`.

  Standard draft-16 publication waits for namespace and per-track readiness,
  emits Moqtail-compatible inline-initialization catalogs, supports subgroup or
  datagram delivery per pad, and refreshes its retained catalog for late
  discovery. Cloudflare draft-14 preserves `.catalog`, separate initialization
  tracks, and subgroup-only publication. Protocol selection is always explicit.

  Raw MoQ Lite draft-05 publication uses exact tracks without an automatically
  generated catalog. Select `profile: :hang` for HANG catalog publication.
  Each Lite input pad must
  provide a positive `timescale`; it may also set track-specific
  `publisher_priority`, `publisher_max_latency`, retention, and reliable
  subgroup delivery. PTS is rounded to the nearest track tick (exact halves
  round up) and must be non-negative and non-decreasing.

      child(:producer, producer)
      |> via_in(Pad.ref(:input, :opus),
        options: [track_name: "opus", timescale: 48_000, retention: :live]
      )
      |> child(:sink, %Membrane.MOQX.Sink{
        endpoint: "moql://cdn.moq.dev:443",
        protocol: :moq_lite_05,
        namespace: ["speech"],
        inbound_subscriptions: :controlled,
        track_demand_events: true
      })

  Selecting HANG catalogs does not add media framing to buffers. Supply already
  framed media through explicit adapters; media decoding and playback remain
  separate verification gates.

  Input EOS finishes the track through MOQX; it does not wait for an application
  acknowledgement that every subscriber received the final buffer. Observed
  Cloudflare draft-14/16 immediate-EOS loss remains a compatibility limitation
  (see `Membrane.MOQX`). Do not treat successful local publication or subscriber
  readiness as delivery certification. This element does not add a delivery
  grace delay, encode/mux payloads, or pace them by PTS.
  """

  use Membrane.Sink

  alias Membrane.MOQX.{ProtocolConventions, Timestamp, Track, Unit}
  alias MOQX.Protocol.Resolver

  def_input_pad :input,
    availability: :on_request,
    flow_control: :auto,
    accepted_format: %Track{},
    options: [
      track_name: [spec: binary(), required: true],
      init_track_name: [spec: binary() | nil, default: nil],
      retention: [spec: :live | :latest | :all, default: :live],
      delivery: [spec: :subgroup | :datagram, default: :subgroup],
      timescale: [spec: pos_integer() | nil, default: nil],
      publisher_priority: [spec: 0..255 | nil, default: nil],
      publisher_max_latency: [spec: non_neg_integer(), default: 0]
    ]

  def_options endpoint: [spec: binary() | URI.t(), required: true],
              protocol: [spec: atom() | module(), required: true],
              profile: [spec: MOQX.Profile.t(), default: :none],
              namespace: [spec: [binary()], required: true],
              authorization: [spec: MOQX.Secret.t() | nil, default: nil],
              timeout: [spec: pos_integer(), default: 5_000],
              connect_options: [spec: keyword(), default: []],
              transport: [spec: term(), default: nil],
              catalog_track_name: [spec: binary() | nil, default: nil],
              publisher_priority: [spec: 0..255, default: 127],
              catalog_refresh_interval: [spec: pos_integer() | nil, default: 1_000],
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
    {:ok, protocol} = Resolver.fetch(options.protocol)
    :ok = MOQX.Profile.validate(options.profile, protocol.id())

    catalog_name =
      case options.profile do
        :none ->
          nil

        profile ->
          {:ok, name} = MOQX.Profile.track_name(profile, :none)
          options.catalog_track_name || name
      end

    state =
      options
      |> Map.from_struct()
      |> Map.put(:catalog_track_name, catalog_name)
      |> Map.merge(%{
        client: nil,
        publication: nil,
        catalog_track: nil,
        catalog_ready?: false,
        catalog_revision: 0,
        catalog_timer: nil,
        pads: %{},
        pending_subscription_requests: %{},
        joining_subscription_requests: %{},
        active_subscriptions: %{},
        subscriber_counts: %{},
        readiness_subscriptions: MapSet.new()
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
          ready?: false,
          pending_notification: nil,
          queued_buffers: [],
          eos_pending?: false,
          generation: 0,
          group_id: 0,
          object_id: 0,
          last_pts: nil,
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
      is_nil(pad_state) ->
        {[], state}

      is_nil(pad_state.track) ->
        case reject_approved_subscriptions_for_track(
               pad_state.options.track_name,
               :track_removed,
               state
             ) do
          {:ok, actions, state} ->
            {actions, state}

          {:error, reason} ->
            raise "failed to reject subscriptions for removed MOQX pad: #{inspect(reason)}"
        end

      pad_state.ended? ->
        {[notify_parent: {:track_removed, pad, pad_state.options.track_name}], state}

      true ->
        with :ok <-
               MOQX.withdraw_track(
                 state.client,
                 pad_state.media_track,
                 status: :track_ended,
                 reason: "track removed"
               ),
             {:ok, state} <- publish_catalog(state) do
          actions = [notify_parent: {:track_removed, pad, pad_state.options.track_name}]
          {actions, state}
        else
          {:error, reason} ->
            raise "failed to finish MOQX track removal: #{inspect(reason)}"
        end
    end
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    cond do
      is_nil(pad_state.track) or pad_state.ended? ->
        {[], state}

      not pad_state.ready? ->
        {[], put_in(state, [:pads, pad, :eos_pending?], true)}

      true ->
        case publish_end_of_track(state, pad_state) do
          :ok ->
            finish_track_eos(pad, pad_state, state)

          {:error, reason} ->
            raise "failed to publish MOQX end-of-track status: #{inspect(reason)}"
        end
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

    case prepare_initialization(state, resolved_init_track_name(options, state), track) do
      {:ok, init_track, init_name} ->
        register_initial_track(pad, pad_state, track, init_track, init_name, state)

      {:error, reason} ->
        raise "failed to prepare MOQX track: #{inspect(reason)}"
    end
  end

  defp register_initial_track(pad, pad_state, track, init_track, init_name, state) do
    options = pad_state.options

    track_options =
      ProtocolConventions.track_options(
        state.protocol,
        options.retention,
        options.delivery,
        timescale: options.timescale,
        publisher_priority: options.publisher_priority || state.publisher_priority,
        publisher_max_latency: options.publisher_max_latency
      )

    case approved_reactive_request(state, options.track_name) do
      nil ->
        add_initial_track(pad, pad_state, track, init_track, init_name, track_options, state)

      request ->
        accept_initial_track(
          pad,
          pad_state,
          track,
          init_track,
          init_name,
          request,
          track_options,
          state
        )
    end
  end

  defp add_initial_track(pad, pad_state, track, init_track, init_name, track_options, state) do
    options = pad_state.options

    case MOQX.add_track(state.client, state.publication, options.track_name, track_options) do
      {:ok, media_track} ->
        ready? = not ProtocolConventions.draft_16?(state.protocol)

        pad_state =
          prepared_pad_state(
            pad_state,
            pad,
            track,
            init_track,
            init_name,
            media_track,
            ready?
          )

        if ready? do
          publish_prepared_track(pad, pad_state, pad_state.pending_notification, state)
        else
          {[], put_in(state, [:pads, pad], pad_state)}
        end

      {:error, reason} ->
        raise "failed to prepare MOQX track: #{inspect(reason)}"
    end
  end

  defp accept_initial_track(
         pad,
         pad_state,
         track,
         init_track,
         init_name,
         request,
         track_options,
         state
       ) do
    case MOQX.accept_subscription(state.client, request, track_options) do
      {:ok, media_track, published_subscription} ->
        state = mark_subscription_joining(request, published_subscription, state)

        pad_state =
          prepared_pad_state(
            pad_state,
            pad,
            track,
            init_track,
            init_name,
            media_track,
            true
          )

        publish_prepared_track(pad, pad_state, pad_state.pending_notification, state)

      {:error, reason} ->
        raise "failed to accept reactive MOQX track: #{inspect(reason)}"
    end
  end

  defp prepared_pad_state(
         pad_state,
         pad,
         track,
         init_track,
         init_name,
         media_track,
         ready?
       ) do
    %{
      pad_state
      | track: track,
        init_track: init_track,
        init_track_name: init_name,
        media_track: media_track,
        ready?: ready?,
        pending_notification: {:track_ready, pad, pad_state.options.track_name}
    }
  end

  defp approved_reactive_request(state, track_name) do
    if ProtocolConventions.draft_16?(state.protocol) or
         ProtocolConventions.moq_lite_05?(state.protocol) do
      Enum.find_value(state.pending_subscription_requests, fn
        {_handle, %{request: %{track: %{track: ^track_name}} = request, status: :approved}} ->
          request

        _entry ->
          nil
      end)
    end
  end

  defp prepare_updated_track(pad, track, state) do
    pad_state = Map.fetch!(state.pads, pad)
    generation = pad_state.generation + 1

    init_name =
      case resolved_init_track_name(pad_state.options, state) do
        nil -> nil
        name -> name <> ".#{generation}"
      end

    case prepare_initialization(state, init_name, track) do
      {:ok, init_track, init_name} ->
        pad_state = %{
          pad_state
          | track: track,
            init_track: init_track,
            init_track_name: init_name,
            generation: generation,
            pending_notification: {:track_updated, pad, pad_state.options.track_name, generation}
        }

        if pad_state.ready? do
          publish_prepared_track(pad, pad_state, pad_state.pending_notification, state)
        else
          {[], put_in(state, [:pads, pad], pad_state)}
        end

      {:error, reason} ->
        raise "failed to update MOQX track: #{inspect(reason)}"
    end
  end

  defp finish_track_eos(pad, pad_state, state) do
    pad_state = %{pad_state | ended?: true}
    state = put_in(state, [:pads, pad], pad_state)

    with :ok <-
           MOQX.withdraw_track(
             state.client,
             pad_state.media_track,
             status: :track_ended,
             reason: "track ended"
           ),
         {:ok, state} <- publish_catalog(state) do
      actions = [notify_parent: {:track_ended, pad, pad_state.options.track_name}]
      {actions, state}
    else
      {:error, reason} ->
        raise "failed to finish MOQX track at end of stream: #{inspect(reason)}"
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

  defp prepare_initialization(%{profile: :none}, _name, _track) do
    {:ok, nil, nil}
  end

  defp prepare_initialization(state, _name, _track)
       when state.protocol in [
              :draft_16,
              MOQX.Protocol.Draft16,
              :moq_lite_05,
              MOQX.Protocol.MOQLite05
            ] do
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

    if pad_state.ready? do
      publish_buffer(pad, buffer, pad_state, state)
    else
      pad_state = update_in(pad_state.queued_buffers, &(&1 ++ [buffer]))
      {[], put_in(state, [:pads, pad], pad_state)}
    end
  end

  defp publish_buffer(pad, buffer, pad_state, state) do
    with {:ok, unit} <- Unit.from_buffer(buffer),
         {:ok, timestamp} <- object_timestamp(buffer, pad_state, state),
         :ok <-
           MOQX.publish_object(state.client, pad_state.media_track, %MOQX.Object{
             group_id: pad_state.group_id,
             subgroup_id: 0,
             object_id: pad_state.object_id,
             timestamp: timestamp,
             publisher_priority: state.publisher_priority,
             end_of_group?: unit.group_end?,
             payload: buffer.payload
           }) do
      pad_state =
        pad_state
        |> Map.put(:last_pts, buffer.pts)
        |> advance_coordinates(unit.group_end?)

      {[], put_in(state, [:pads, pad], pad_state)}
    else
      {:error, reason} -> raise "failed to publish MOQX object: #{inspect(reason)}"
    end
  end

  defp object_timestamp(_buffer, _pad_state, state)
       when state.protocol not in [:moq_lite_05, MOQX.Protocol.MOQLite05],
       do: {:ok, nil}

  defp object_timestamp(buffer, pad_state, _state) do
    Timestamp.from_membrane_time(buffer.pts, pad_state.options.timescale, pad_state.last_pts)
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

  def handle_parent_notification(
        {:finish_subscription, request_handle, options},
        _ctx,
        state
      )
      when is_list(options) do
    case find_active_subscription(state, request_handle) do
      nil ->
        notification =
          {:subscription_finish_failed, request_handle, :unknown_published_subscription}

        {[notify_parent: notification], state}

      published_subscription ->
        options = finish_subscription_options(state, options)

        case MOQX.finish_subscription(state.client, published_subscription, options) do
          :ok ->
            {[], state}

          {:error, reason} ->
            notification = {:subscription_finish_failed, request_handle, reason}
            {[notify_parent: notification], state}
        end
    end
  end

  def handle_parent_notification(_notification, _ctx, state), do: {[], state}

  @impl true
  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationReady{publication: publication}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    if is_nil(state.catalog_track_name) do
      publication_became_ready(%{state | catalog_ready?: true})
    else
      register_catalog_track(client, publication, state)
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
         %MOQX.Event.PublicationSubscriberJoined{
           track: track,
           subscription: published_subscription,
           request_id: request_id
         }},
        _ctx,
        %{client: client} = state
      ) do
    if ProtocolConventions.draft_16?(state.protocol) and published_track_pending?(track, state) do
      published_track_became_ready(track, request_id, state)
    else
      subscriber_joined(track, published_subscription, request_id, state)
    end
  end

  def handle_info(
        {:moqx, client,
         %MOQX.Event.PublicationSubscriberLeft{
           track: track,
           subscription: published_subscription,
           request_id: request_id
         }},
        _ctx,
        %{client: client} = state
      ) do
    key = {track, request_id}

    if MapSet.member?(state.readiness_subscriptions, key) do
      {[], %{state | readiness_subscriptions: MapSet.delete(state.readiness_subscriptions, key)}}
    else
      subscriber_left(track, published_subscription, request_id, state)
    end
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationFailed{publication: publication, error: error}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    terminate_after_event({:publication_failed, error}, state, :close)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationTrackFailed{track: track, error: error}},
        _ctx,
        %{client: client} = state
      ) do
    notification = {:publication_track_failed, published_track_name(track), error}
    terminate_after_event(notification, state, :close)
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

  def handle_info(:refresh_catalog, _ctx, state) do
    state = %{state | catalog_timer: nil}

    case publish_catalog(state) do
      {:ok, state} -> {[], state}
      {:error, reason} -> raise "failed to refresh MOQX catalog: #{inspect(reason)}"
    end
  end

  def handle_info(_message, _ctx, state), do: {[], state}

  defp register_catalog_track(client, publication, state) do
    options = [
      profile: if(state.profile == :hang, do: :hang, else: :none),
      retention: :latest,
      timescale: 1_000_000,
      publisher_priority:
        ProtocolConventions.catalog_priority(state.protocol, state.publisher_priority)
    ]

    case MOQX.add_track(client, publication, state.catalog_track_name, options) do
      {:ok, catalog_track} ->
        state = %{
          state
          | catalog_track: catalog_track,
            catalog_ready?: not ProtocolConventions.draft_16?(state.protocol)
        }

        catalog_track_registered(state)

      {:error, reason} ->
        raise "failed to register MOQX catalog track: #{inspect(reason)}"
    end
  end

  defp catalog_track_registered(%{catalog_ready?: true} = state),
    do: publication_became_ready(state)

  defp catalog_track_registered(state), do: {[], state}

  defp subscriber_joined(track, published_subscription, request_id, state) do
    track_name = published_track_name(track)
    {identity, state} = take_joining_subscription(state, published_subscription, request_id)
    previous_count = Map.get(state.subscriber_counts, track_name, 0)
    count = previous_count + 1

    state =
      state
      |> put_in(
        [:active_subscriptions, published_subscription],
        %{identity: identity, track_name: track_name}
      )
      |> put_in([:subscriber_counts, track_name], count)

    notification = {:subscriber_joined, track_name, identity, count}

    actions =
      [notify_parent: notification] ++
        track_demand_actions(state, track_name, previous_count, count)

    {actions, state}
  end

  defp subscriber_left(track, published_subscription, request_id, state) do
    track_name = published_track_name(track)

    {active_subscription, active} =
      Map.pop(state.active_subscriptions, published_subscription)

    identity =
      case active_subscription do
        %{identity: identity} -> identity
        nil -> request_id
      end

    previous_count = Map.get(state.subscriber_counts, track_name, 1)
    count = max(previous_count - 1, 0)

    state = %{
      state
      | active_subscriptions: active,
        subscriber_counts: Map.put(state.subscriber_counts, track_name, count)
    }

    notification = {:subscriber_left, track_name, identity, count}

    actions =
      [notify_parent: notification] ++
        track_demand_actions(state, track_name, previous_count, count)

    {actions, state}
  end

  defp published_track_became_ready(track, request_id, %{catalog_track: track} = state) do
    state =
      state
      |> Map.put(:catalog_ready?, true)
      |> remember_readiness_subscription(track, request_id)

    publication_became_ready(state)
  end

  defp published_track_became_ready(track, request_id, state) do
    state = remember_readiness_subscription(state, track, request_id)

    case pad_for_published_track(state, track) do
      nil ->
        {[], state}

      pad ->
        pad_state =
          state.pads
          |> Map.fetch!(pad)
          |> Map.put(:ready?, true)

        state = put_in(state, [:pads, pad], pad_state)

        {actions, state} =
          publish_prepared_track(
            pad,
            pad_state,
            pad_state.pending_notification,
            state
          )

        flush_ready_pad(pad, actions, state)
    end
  end

  @impl true
  def handle_terminate_request(_ctx, state) do
    cancel_catalog_refresh(state)
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

  defp publication_became_ready(state) do
    state =
      if state.profile == :hang do
        {:ok, state} = publish_catalog(state)
        state
      else
        state
      end

    {subscription_actions, state} =
      accept_approved_subscriptions([state.catalog_track_name], state)

    actions =
      [setup: :complete, notify_parent: {:publication_ready, state.namespace}] ++
        subscription_actions

    {actions, state}
  end

  defp published_track_pending?(track, state) do
    (track == state.catalog_track and not state.catalog_ready?) or
      Enum.any?(state.pads, fn {_pad, pad_state} ->
        pad_state.media_track == track and not pad_state.ready?
      end)
  end

  defp remember_readiness_subscription(state, track, request_id) do
    update_in(state.readiness_subscriptions, &MapSet.put(&1, {track, request_id}))
  end

  defp pad_for_published_track(state, track) do
    Enum.find_value(state.pads, fn {pad, pad_state} ->
      if pad_state.media_track == track, do: pad
    end)
  end

  defp flush_ready_pad(pad, actions, state) do
    pad_state = Map.fetch!(state.pads, pad)
    queued_buffers = pad_state.queued_buffers
    eos_pending? = pad_state.eos_pending?

    state =
      put_in(state, [:pads, pad], %{
        pad_state
        | queued_buffers: [],
          eos_pending?: false,
          pending_notification: nil
      })

    {actions, state} =
      Enum.reduce(queued_buffers, {actions, state}, fn buffer, {actions, state} ->
        pad_state = Map.fetch!(state.pads, pad)
        {next_actions, state} = publish_buffer(pad, buffer, pad_state, state)
        {actions ++ next_actions, state}
      end)

    if eos_pending? do
      pad_state = Map.fetch!(state.pads, pad)

      case publish_end_of_track(state, pad_state) do
        :ok ->
          {eos_actions, state} = finish_track_eos(pad, pad_state, state)
          {actions ++ eos_actions, state}

        {:error, reason} ->
          raise "failed to publish queued MOQX end-of-track: #{inspect(reason)}"
      end
    else
      {actions, state}
    end
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
      {:ok, published_subscription} ->
        {[], mark_subscription_joining(request, published_subscription, state)}

      {:error, reason} ->
        notification = {:subscription_decision_failed, request, reason}
        {[notify_parent: notification], state}
    end
  end

  defp mark_subscription_joining(request, published_subscription, state) do
    state
    |> update_in([:pending_subscription_requests], &Map.delete(&1, request.handle))
    |> put_in([:joining_subscription_requests, published_subscription], request.handle)
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

  defp reject_approved_subscriptions_for_track(track_name, cancellation_reason, state) do
    state.pending_subscription_requests
    |> Enum.filter(fn {_handle, pending} ->
      pending.status == :approved and pending.request.track.track == track_name
    end)
    |> Enum.reduce_while({:ok, [], state}, fn {_handle, pending}, {:ok, actions, state} ->
      rejection = %MOQX.SubscriptionRejection{
        code: :track_does_not_exist,
        reason: "track removed before registration"
      }

      case MOQX.reject_subscription(state.client, pending.request, rejection) do
        :ok ->
          state = drop_pending_subscription_request(state, pending.request)

          notification = {:subscription_cancelled, pending.request, cancellation_reason}
          {:cont, {:ok, actions ++ [notify_parent: notification], state}}

        {:error, :stale_subscription_request} ->
          state = drop_pending_subscription_request(state, pending.request)
          {:cont, {:ok, actions, state}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp drop_pending_subscription_request(state, request) do
    update_in(state.pending_subscription_requests, &Map.delete(&1, request.handle))
  end

  defp take_joining_subscription(state, published_subscription, request_id) do
    {identity, joining} =
      Map.pop(state.joining_subscription_requests, published_subscription, request_id)

    {identity, %{state | joining_subscription_requests: joining}}
  end

  defp find_active_subscription(state, request_handle) do
    Enum.find_value(state.active_subscriptions, fn
      {published_subscription, %{identity: ^request_handle}} -> published_subscription
      _entry -> nil
    end)
  end

  defp finish_subscription_options(state, _options)
       when state.protocol in [:moq_lite_05, MOQX.Protocol.MOQLite05],
       do: []

  defp finish_subscription_options(_state, options), do: options

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
        resolved_init_track_name(pad_state.options, state) == track_name
      end)
  end

  defp init_track_name(%{init_track_name: nil, track_name: track_name}),
    do: track_name <> ".init"

  defp init_track_name(%{init_track_name: name}), do: name

  defp resolved_init_track_name(_options, %{profile: profile})
       when profile != :cloudflare_cmsf,
       do: nil

  defp resolved_init_track_name(options, _state), do: init_track_name(options)

  defp validate_pad_track_names(options, state) do
    track_name = options.track_name
    init_name = resolved_init_track_name(options, state)

    media_names = Enum.map(state.pads, fn {_pad, pad_state} -> pad_state.options.track_name end)

    init_names =
      Enum.map(state.pads, fn {_pad, pad_state} ->
        resolved_init_track_name(pad_state.options, state)
      end)

    with :ok <- validate_lite_pad_options(options, state),
         :ok <-
           validate_media_track_name(
             track_name,
             state.catalog_track_name,
             media_names,
             init_names
           ) do
      validate_init_track_name(
        init_name,
        track_name,
        state.catalog_track_name,
        media_names,
        init_names
      )
    end
  end

  defp validate_lite_pad_options(options, state) do
    if ProtocolConventions.moq_lite_05?(state.protocol) do
      cond do
        not is_integer(options.timescale) or options.timescale <= 0 ->
          {:error, :invalid_timescale}

        options.delivery != :subgroup ->
          {:error, {:unsupported_delivery, options.delivery}}

        true ->
          :ok
      end
    else
      :ok
    end
  end

  defp validate_media_track_name(track_name, catalog_name, media_names, init_names) do
    cond do
      track_name == catalog_name -> {:error, {:reserved_track_name, track_name}}
      track_name in media_names -> {:error, {:duplicate_track_name, track_name}}
      track_name in init_names -> {:error, {:track_name_conflicts_with_init_track, track_name}}
      true -> :ok
    end
  end

  defp validate_init_track_name(nil, _track_name, _catalog_name, _media_names, _init_names),
    do: :ok

  defp validate_init_track_name(init_name, track_name, catalog_name, media_names, init_names) do
    cond do
      init_name == track_name or init_name == catalog_name or init_name in media_names ->
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
    if ProtocolConventions.moq_lite_05?(state.protocol) do
      :ok
    else
      MOQX.publish_object(state.client, pad_state.media_track, %MOQX.Object{
        group_id: pad_state.group_id,
        subgroup_id: 0,
        object_id: pad_state.object_id,
        publisher_priority: state.publisher_priority,
        status: :end_of_track,
        payload: <<>>
      })
    end
  end

  defp publish_catalog(state) when is_nil(state.catalog_track_name), do: {:ok, state}

  defp publish_catalog(state) do
    case catalog_snapshot(state) do
      {:ok, catalog} -> publish_catalog_snapshot(catalog, state)
      {:error, reason} -> {:error, reason}
    end
  end

  defp catalog_snapshot(%{profile: :hang} = state) do
    catalog_tracks(state)
    |> Enum.reduce_while({:ok, %{}}, fn {name, _init, track}, {:ok, raw} ->
      case hang_rendition(track) do
        {:ok, role, rendition} ->
          section = Map.get(raw, role, %{"renditions" => %{}})
          section = update_in(section["renditions"], &Map.put(&1, name, rendition))
          {:cont, {:ok, Map.put(raw, role, section)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, raw} ->
        MOQX.Catalog.decode(JSON.encode!(raw), format: :hang, namespace: state.namespace)

      {:error, _} = error ->
        error
    end
  end

  defp catalog_snapshot(state) do
    convention = if state.profile == :moqtail_cmsf, do: :draft_16, else: :cloudflare_draft_14
    format = if state.profile == :moqtail_cmsf, do: :moqtail_cmsf, else: :cloudflare
    raw = ProtocolConventions.catalog(convention, state.namespace, catalog_tracks(state))
    # CMSF encoding uses its raw representation; receiving codec validation is
    # intentionally not a restriction on the plugin's open packaging contract.
    {:ok, %MOQX.Catalog{format: format, tracks: [], raw: raw, namespace: state.namespace}}
  end

  defp hang_rendition(%Track{packaging: "hang/legacy"} = track) do
    role = track.catalog_fields["role"]

    raw =
      track.catalog_fields
      |> Map.delete("role")
      |> Map.merge(track.selection_params)
      |> Map.put("container", %{"kind" => "legacy"})

    raw =
      if is_binary(track.initialization),
        do: Map.put(raw, "description", Base.encode16(track.initialization, case: :lower)),
        else: raw

    if role in ["audio", "video"], do: {:ok, role, raw}, else: {:error, :hang_media_role_required}
  end

  defp hang_rendition(%Track{packaging: "cmaf", initialization: init} = track)
       when is_binary(init) do
    role = track.catalog_fields["role"]

    raw =
      track.catalog_fields
      |> Map.delete("role")
      |> Map.merge(track.selection_params)
      |> Map.put("container", %{"kind" => "cmaf", "init" => Base.encode64(init)})

    if role in ["audio", "video"], do: {:ok, role, raw}, else: {:error, :hang_media_role_required}
  end

  defp hang_rendition(_track), do: {:error, :unsupported_hang_packaging}

  defp publish_catalog_snapshot(catalog, state) do
    result =
      if state.profile == :hang do
        MOQX.publish_catalog(state.client, state.catalog_track, catalog)
      else
        # MOQX's catalog publication API has no per-object priority option.
        # CMSF draft-16 catalogs require our existing priority-zero convention.
        with {:ok, payload} <- MOQX.Catalog.encode(catalog) do
          MOQX.publish_object(state.client, state.catalog_track, %MOQX.Object{
            group_id: state.catalog_revision,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority:
              ProtocolConventions.catalog_priority(state.protocol, state.publisher_priority),
            end_of_group?: true,
            payload: payload
          })
        end
      end

    case result do
      :ok ->
        state = %{state | catalog_revision: state.catalog_revision + 1}
        {:ok, schedule_catalog_refresh(state)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp catalog_tracks(state) do
    state.pads
    |> Enum.filter(fn {_pad, pad_state} ->
      not is_nil(pad_state.track) and !pad_state.ended?
    end)
    |> Enum.sort_by(fn {_pad, pad_state} -> pad_state.options.track_name end)
    |> Enum.map(fn {_pad, pad_state} ->
      {pad_state.options.track_name, pad_state.init_track_name, pad_state.track}
    end)
  end

  defp schedule_catalog_refresh(state) do
    cancel_catalog_refresh(state)

    if ProtocolConventions.draft_16?(state.protocol) and
         is_integer(state.catalog_refresh_interval) do
      ref = Process.send_after(self(), :refresh_catalog, state.catalog_refresh_interval)
      %{state | catalog_timer: ref}
    else
      %{state | catalog_timer: nil}
    end
  end

  defp cancel_catalog_refresh(%{catalog_timer: nil}), do: :ok
  defp cancel_catalog_refresh(%{catalog_timer: ref}), do: Process.cancel_timer(ref)

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
    cancel_catalog_refresh(state)
    close_client(state)
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  defp terminate_after_event(notification, state, :already_closed) do
    cancel_catalog_refresh(state)
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end
end
