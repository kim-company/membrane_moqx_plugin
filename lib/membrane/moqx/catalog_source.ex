defmodule Membrane.MOQX.CatalogSource do
  @moduledoc """
  Discovers a namespace catalog and creates one-track Sources on dynamic pads.

  Catalog offers are parent notifications, not automatic links. Linking an
  exact output pad requests that track. Callers may also request an
  unadvertised track by supplying its canonical stream format in pad options.

  Select `profile` independently from `protocol`: `:moqtail_cmsf` defaults to
  `catalog`, `:cloudflare_cmsf` to `.catalog`, and `:hang` to `catalog.json`.
  MOQX validates supported compositions; HANG currently requires Lite05.
  A catalog name override changes the address, never the schema. `:none` is
  rejected; use `Membrane.MOQX.Source` for an opaque exact track.

  CMSF inline initialization is preserved; Cloudflare `initTrack` values retain
  their separate subscription path. HANG decoder metadata becomes canonical
  selection parameters. CMAF retains its packaging; other HANG containers use
  `hang/<kind>` packaging and require explicit media adapters before decoding.
  A discovered offer is not a guarantee of playback compatibility.

  Malformed snapshots emit `{:catalog_failed, error}` without discarding the
  last valid offers or terminating the pipeline. A subscription failure, in
  contrast, emits `{:catalog_failed, error}` and terminates this catalog source.
  Connection loss emits `{:connection_closed, metadata}` before termination.
  There is no automatic reconnect: a parent that wants recovery must isolate
  the child in a crash group and create a replacement. Later valid snapshots
  may replace the offered track metadata; existing linked Sources remain pipeline-owned.
  An unsupported rendition emits `{:track_ignored, name, reason}` and invalidates
  any old offer for that address with `{:track_unavailable, offer}`. Removal
  uses MOQX's resolved address, including relative cross-broadcast references.

  This module discovers tracks within a selected broadcast. Discovering the
  broadcasts themselves is a separate `Membrane.MOQX.Session.discover/3`
  operation; the application decides which catalogs to follow.
  """

  use Membrane.Bin

  import Membrane.ChildrenSpec

  alias Membrane.MOQX.{Session, Source, Track, TrackOffer}
  alias MOQX.Protocol.Resolver

  def_output_pad :output,
    availability: :on_request,
    accepted_format: %Track{},
    options: [
      track: [spec: MOQX.TrackRef.t(), required: true],
      stream_format: [spec: Track.t() | nil, default: nil],
      start_policy: [spec: :current | :next_group, default: :current],
      subscription_options: [spec: keyword(), default: []]
    ]

  def_options endpoint: [spec: binary() | URI.t(), required: true],
              protocol: [spec: atom() | module(), required: true],
              profile: [spec: MOQX.Profile.t(), default: :none],
              namespace: [spec: [binary()], required: true],
              authorization: [spec: MOQX.Secret.t() | nil, default: nil],
              timeout: [spec: pos_integer(), default: 5_000],
              connect_options: [spec: keyword(), default: []],
              transport: [spec: term(), default: nil],
              catalog_track_name: [spec: binary() | nil, default: nil]

  @impl true
  def handle_init(_ctx, options) do
    if options.profile == :none do
      raise ArgumentError, "CatalogSource requires a catalog profile; use Source for raw tracks"
    end

    {:ok, protocol} = Resolver.fetch(options.protocol)
    :ok = MOQX.Profile.validate(options.profile, protocol.id())
    {:ok, default_name} = MOQX.Profile.track_name(options.profile, :none)
    catalog_track_name = options.catalog_track_name || default_name

    state =
      options
      |> Map.from_struct()
      |> Map.put(:catalog_track_name, catalog_track_name)
      |> Map.merge(%{
        session: nil,
        catalog_subscription: nil,
        advertised: %{},
        offers: %{},
        init_subscriptions: %{},
        initialization_cache: %{},
        pads: %{}
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    with {:ok, session} <- Session.start_link(session_options(state)),
         catalog_ref = %MOQX.TrackRef{
           namespace: state.namespace,
           track: state.catalog_track_name
         },
         {:ok, subscription} <- Session.subscribe(session, catalog_ref, profile: state.profile) do
      {[setup: :incomplete], %{state | session: session, catalog_subscription: subscription}}
    else
      {:error, reason} -> raise "failed to start MOQX catalog source: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    track_ref = ctx.pad_options.track
    offer = state.offers[track_ref]
    stream_format = ctx.pad_options.stream_format || (offer && offer.stream_format)

    pad_state = %{
      track_ref: track_ref,
      stream_format: stream_format,
      options: ctx.pad_options,
      child: nil
    }

    state = put_in(state, [:pads, pad], pad_state)
    request_action = {:notify_parent, {:track_requested, pad, track_ref}}

    case stream_format do
      %Track{} ->
        {spec, pad_state} = source_spec(pad, pad_state, state)
        {[request_action, spec: spec], put_in(state, [:pads, pad], pad_state)}

      nil ->
        {[request_action], state}
    end
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    case Map.pop(state.pads, pad) do
      {nil, _pads} ->
        {[], state}

      {%{child: nil}, pads} ->
        {[], %{state | pads: pads}}

      {%{child: child}, pads} ->
        {[remove_child: child], %{state | pads: pads}}
    end
  end

  @impl true
  def handle_info(
        {:moqx_session, session, %MOQX.Event.SubscriptionAccepted{subscription: subscription}},
        _ctx,
        %{session: session, catalog_subscription: subscription} = state
      ) do
    {[setup: :complete, notify_parent: :catalog_ready], state}
  end

  def handle_info(
        {:moqx_session, session,
         %MOQX.Event.CatalogFailed{subscription: subscription, error: error}},
        _ctx,
        %{session: session, catalog_subscription: subscription} = state
      ) do
    {[notify_parent: {:catalog_failed, error}], state}
  end

  def handle_info(
        {:moqx_session, session, %MOQX.Event.CatalogReceived{catalog: catalog}},
        _ctx,
        %{session: session} = state
      ) do
    update_catalog(catalog, state)
  end

  def handle_info(
        {:moqx_session, session,
         %MOQX.Event.ObjectReceived{object: %{subscription: subscription} = object}},
        _ctx,
        %{session: session} = state
      ) do
    receive_initialization(subscription, object.payload, state)
  end

  def handle_info(
        {:moqx_session, session,
         %MOQX.Event.SubscriptionFailed{subscription: subscription, error: error}},
        _ctx,
        %{session: session, catalog_subscription: subscription} = state
      ) do
    notification = {:catalog_failed, error}
    {[setup: :complete, notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  def handle_info(
        {:moqx_session, session, %MOQX.Event.ConnectionClosed{metadata: metadata}},
        _ctx,
        %{session: session} = state
      ) do
    notification = {:connection_closed, metadata}
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  def handle_info(
        {:moqx_session, session, %MOQX.Event.ProtocolFailed{reason: reason}},
        _ctx,
        %{session: session} = state
      ) do
    notification = {:protocol_failed, reason}
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  def handle_info(_message, _ctx, state), do: {[], state}

  @impl true
  def handle_child_notification(notification, child, _ctx, state) do
    track_ref =
      Enum.find_value(state.pads, fn {_pad, pad_state} ->
        if pad_state.child == child, do: pad_state.track_ref
      end)

    {[notify_parent: {:track_source, track_ref, notification}], state}
  end

  @impl true
  def handle_terminate_request(_ctx, state) do
    if is_pid(state.session) and Process.alive?(state.session), do: Session.close(state.session)
    {[terminate: :normal], state}
  end

  defp update_catalog(catalog, state) do
    advertised = Map.new(catalog.tracks, &{catalog_track_ref(&1, state.namespace), &1})
    removed_refs = Map.keys(state.advertised) -- Map.keys(advertised)

    removed_actions =
      Enum.map(removed_refs, fn track_ref ->
        offer = state.offers[track_ref]
        {:notify_parent, {:track_unavailable, offer || track_ref}}
      end)

    state = %{
      state
      | advertised: advertised,
        offers: Map.drop(state.offers, removed_refs)
    }

    Enum.reduce(catalog.tracks, {removed_actions, state}, fn catalog_track, {actions, state} ->
      {next_actions, state} = prepare_offer(catalog_track, state)
      {actions ++ next_actions, state}
    end)
  end

  defp prepare_offer(%{init_track: init_track} = catalog_track, state)
       when is_binary(init_track) do
    init_ref = %MOQX.TrackRef{namespace: state.namespace, track: init_track}

    case state.initialization_cache[init_ref] do
      nil ->
        subscribe_initialization(catalog_track, init_ref, state)

      initialization ->
        publish_offer(catalog_track, initialization, state)
    end
  end

  defp prepare_offer(catalog_track, state), do: publish_offer(catalog_track, nil, state)

  defp subscribe_initialization(catalog_track, init_ref, state) do
    already_pending? =
      Enum.any?(state.init_subscriptions, fn {_sub, value} -> value == catalog_track end)

    if already_pending? do
      {[], state}
    else
      case Session.subscribe(state.session, init_ref) do
        {:ok, subscription} ->
          {[], put_in(state, [:init_subscriptions, subscription], catalog_track)}

        {:error, reason} ->
          {[notify_parent: {:initialization_failed, init_ref, reason}], state}
      end
    end
  end

  defp receive_initialization(subscription, payload, state) do
    case Map.pop(state.init_subscriptions, subscription) do
      {nil, _subscriptions} ->
        {[], state}

      {catalog_track, subscriptions} ->
        _result = Session.unsubscribe(state.session, subscription)
        init_ref = %MOQX.TrackRef{namespace: state.namespace, track: catalog_track.init_track}

        state = %{
          state
          | init_subscriptions: subscriptions,
            initialization_cache: Map.put(state.initialization_cache, init_ref, payload)
        }

        publish_offer(catalog_track, payload, state)
    end
  end

  defp publish_offer(catalog_track, initialization, state) do
    case TrackOffer.from_catalog_track(catalog_track, state.namespace, initialization) do
      {:ok, offer} ->
        old_offer = state.offers[offer.track_ref]
        state = put_in(state, [:offers, offer.track_ref], offer)

        offer_actions =
          if old_offer == offer, do: [], else: [notify_parent: {:track_available, offer}]

        {pad_actions, state} = start_waiting_pads(offer, state)
        {offer_actions ++ pad_actions, state}

      {:error, reason} ->
        ref = catalog_track_ref(catalog_track, state.namespace)
        {old_offer, offers} = Map.pop(state.offers, ref)
        actions = [notify_parent: {:track_ignored, catalog_track.name, reason}]

        actions =
          if old_offer,
            do: actions ++ [notify_parent: {:track_unavailable, old_offer}],
            else: actions

        {actions, %{state | offers: offers}}
    end
  end

  defp start_waiting_pads(offer, state) do
    Enum.reduce(state.pads, {[], state}, fn
      {pad, %{track_ref: track_ref, child: nil, stream_format: nil} = pad_state}, {actions, state}
      when track_ref == offer.track_ref ->
        pad_state = %{pad_state | stream_format: offer.stream_format}
        {spec, pad_state} = source_spec(pad, pad_state, state)
        {actions ++ [spec: spec], put_in(state, [:pads, pad], pad_state)}

      {_pad, _pad_state}, acc ->
        acc
    end)
  end

  defp source_spec(pad, pad_state, state) do
    child = {:track_source, pad}

    spec =
      child(child, %Source{
        session: state.session,
        protocol: state.protocol,
        track: pad_state.track_ref,
        stream_format: pad_state.stream_format,
        start_policy: pad_state.options.start_policy,
        subscription_options: pad_state.options.subscription_options
      })
      |> bin_output(pad)

    {spec, %{pad_state | child: child}}
  end

  defp catalog_track_ref(catalog_track, namespace) do
    MOQX.Catalog.Track.track_ref(catalog_track, namespace)
  end

  defp session_options(state) do
    [
      endpoint: state.endpoint,
      protocol: state.protocol,
      authorization: state.authorization,
      timeout: state.timeout,
      connect_options: state.connect_options,
      transport: state.transport
    ]
  end
end
