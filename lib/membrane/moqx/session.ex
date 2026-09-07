defmodule Membrane.MOQX.Session do
  @moduledoc """
  Owns one bidirectional MOQX client and routes subscription events by owner.

  A catalog controller and its one-track Sources can share this process without
  sharing their Membrane mailboxes. Each subscription is monitored and is
  automatically cancelled if its owner exits.

  This process shares connection lifetime and routes subscriptions and broadcast
  discoveries to their individual callers. Catalog profiles are passed per
  subscription to MOQX; they are not connection-global. Discovery reports
  paths, never automatically subscribes to catalogs or creates media Sources.
  Subscription/discovery owner exit cancels only that owner's handles.
  Its protocol is fixed at startup; Sources sharing it
  must select the same resolved protocol. It does not automatically retry a
  different draft when a relay rejects the selected one.
  """

  use GenServer

  @type option ::
          {:endpoint, binary() | URI.t()}
          | {:protocol, atom() | module()}
          | {:authorization, MOQX.Secret.t()}
          | {:timeout, pos_integer()}
          | {:connect_options, keyword()}
          | {:transport, term()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @spec subscribe(pid(), MOQX.TrackRef.t(), keyword()) ::
          {:ok, MOQX.Subscription.t()} | {:error, term()}
  def subscribe(session, track, options \\ []) do
    GenServer.call(session, {:subscribe, track, options})
  end

  @spec unsubscribe(pid(), MOQX.Subscription.t()) :: :ok | {:error, term()}
  def unsubscribe(session, subscription) do
    GenServer.call(session, {:unsubscribe, subscription})
  end

  @doc "Discovers broadcast paths, routing MOQX discovery events to the caller."
  @spec discover(pid(), binary(), keyword()) :: {:ok, MOQX.Discovery.t()} | {:error, term()}
  def discover(session, prefix, options \\ []),
    do: GenServer.call(session, {:discover, prefix, options})

  @doc "Cancels a discovery owned by the caller; withdrawal and completion remain observable."
  @spec cancel_discovery(pid(), MOQX.Discovery.t()) :: :ok | {:error, term()}
  def cancel_discovery(session, discovery),
    do: GenServer.call(session, {:cancel_discovery, discovery})

  @doc "Returns the explicit protocol selection owned by the session."
  @spec protocol(pid()) :: atom() | module()
  def protocol(session), do: GenServer.call(session, :protocol)

  @spec close(pid()) :: :ok
  def close(session), do: GenServer.call(session, :close)

  @impl true
  def init(options) do
    endpoint = Keyword.fetch!(options, :endpoint)
    protocol = Keyword.fetch!(options, :protocol)

    connect_options =
      [
        protocol: protocol,
        events_to: self(),
        timeout: Keyword.get(options, :timeout, 5_000),
        connect_options: Keyword.get(options, :connect_options, [])
      ]
      |> put_if_present(:authorization, Keyword.get(options, :authorization))
      |> put_if_present(:transport, Keyword.get(options, :transport))

    case MOQX.connect(endpoint, connect_options) do
      {:ok, client} ->
        {:ok,
         %{
           client: client,
           protocol: protocol,
           subscriptions: %{},
           discoveries: %{},
           monitors: %{},
           closed?: false
         }}

      {:error, reason} ->
        {:stop, {:connection_failed, reason}}
    end
  end

  @impl true
  def handle_call(:protocol, _from, state), do: {:reply, state.protocol, state}

  def handle_call({:discover, prefix, options}, {owner, _tag}, state) do
    case MOQX.discover(state.client, prefix, options) do
      {:ok, discovery} ->
        monitor = Process.monitor(owner)
        entry = %{owner: owner, monitor: monitor}

        state = %{
          state
          | discoveries: Map.put(state.discoveries, discovery, entry),
            monitors: Map.put(state.monitors, monitor, {:discovery, discovery})
        }

        {:reply, {:ok, discovery}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel_discovery, discovery}, {owner, _tag}, state) do
    case state.discoveries[discovery] do
      %{owner: ^owner} -> {:reply, MOQX.cancel_discovery(state.client, discovery), state}
      nil -> {:reply, :ok, state}
      _ -> {:reply, {:error, :not_discovery_owner}, state}
    end
  end

  def handle_call({:subscribe, track, options}, {owner, _tag}, state) do
    case MOQX.subscribe(state.client, track, options) do
      {:ok, subscription} ->
        monitor = Process.monitor(owner)

        entry = %{owner: owner, monitor: monitor}

        state = %{
          state
          | subscriptions: Map.put(state.subscriptions, subscription, entry),
            monitors: Map.put(state.monitors, monitor, subscription)
        }

        {:reply, {:ok, subscription}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:unsubscribe, subscription}, {owner, _tag}, state) do
    case state.subscriptions[subscription] do
      %{owner: ^owner} ->
        result = normalize_unsubscribe(MOQX.unsubscribe(state.client, subscription))
        {:reply, result, drop_subscription(state, subscription)}

      nil ->
        {:reply, :ok, state}

      _other_owner ->
        {:reply, {:error, :not_subscription_owner}, state}
    end
  end

  def handle_call(:close, _from, state) do
    result = normalize_close(MOQX.close(state.client))
    {:stop, :normal, result, %{state | closed?: true}}
  end

  @impl true
  def handle_info({:moqx, client, event}, %{client: client} = state) do
    {state, recipients} = event_recipients(event, state)
    Enum.each(recipients, &send(&1, {:moqx_session, self(), event}))
    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, _owner, _reason}, state) do
    case state.monitors[monitor] do
      {:discovery, discovery} ->
        _result = MOQX.cancel_discovery(state.client, discovery)
        {:noreply, drop_discovery(state, discovery, false)}

      nil ->
        {:noreply, state}

      subscription ->
        _result = MOQX.unsubscribe(state.client, subscription)
        {:noreply, drop_subscription(state, subscription, false)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{closed?: false, client: client}) do
    _result = MOQX.close(client)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp event_recipients(%MOQX.Event.ConnectionClosed{}, state),
    do: {state, all_owners(state)}

  defp event_recipients(%MOQX.Event.ProtocolFailed{}, state),
    do: {state, all_owners(state)}

  defp event_recipients(%module{discovery: discovery} = event, state)
       when module in [
              MOQX.Event.DiscoveryReady,
              MOQX.Event.BroadcastAvailable,
              MOQX.Event.BroadcastWithdrawn,
              MOQX.Event.DiscoveryDone
            ] do
    case state.discoveries[discovery] do
      nil ->
        {state, []}

      %{owner: owner} ->
        state =
          if match?(%MOQX.Event.DiscoveryDone{}, event),
            do: drop_discovery(state, discovery),
            else: state

        {state, [owner]}
    end
  end

  defp event_recipients(event, state) do
    case event_subscription(event) do
      nil ->
        {state, []}

      subscription ->
        route_subscription_event(event, subscription, state)
    end
  end

  defp route_subscription_event(event, subscription, state) do
    case state.subscriptions[subscription] do
      nil ->
        {state, []}

      %{owner: owner} ->
        state =
          if terminal_event?(event),
            do: drop_subscription(state, subscription),
            else: state

        {state, [owner]}
    end
  end

  defp event_subscription(%MOQX.Event.SubscriptionAccepted{subscription: subscription}),
    do: subscription

  defp event_subscription(%MOQX.Event.CatalogReceived{subscription: subscription}),
    do: subscription

  defp event_subscription(%MOQX.Event.CatalogFailed{subscription: subscription}),
    do: subscription

  defp event_subscription(%MOQX.Event.SubscriptionFailed{subscription: subscription}),
    do: subscription

  defp event_subscription(%MOQX.Event.SubscriptionDone{subscription: subscription}),
    do: subscription

  defp event_subscription(%MOQX.Event.ObjectReceived{object: object}), do: object.subscription
  defp event_subscription(%MOQX.Event.ObjectStatus{object: object}), do: object.subscription

  defp event_subscription(%MOQX.Event.SubgroupEnded{subscription: subscription}),
    do: subscription

  defp event_subscription(_event), do: nil

  defp terminal_event?(%MOQX.Event.SubscriptionFailed{}), do: true
  defp terminal_event?(%MOQX.Event.SubscriptionDone{}), do: true
  defp terminal_event?(_event), do: false

  defp drop_subscription(state, subscription, demonitor? \\ true) do
    case Map.pop(state.subscriptions, subscription) do
      {nil, _subscriptions} ->
        state

      {%{monitor: monitor}, subscriptions} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])

        %{
          state
          | subscriptions: subscriptions,
            monitors: Map.delete(state.monitors, monitor)
        }
    end
  end

  defp all_owners(state) do
    (Map.values(state.subscriptions) ++ Map.values(state.discoveries))
    |> Enum.map(& &1.owner)
    |> Enum.uniq()
  end

  defp drop_discovery(state, discovery, demonitor? \\ true) do
    case Map.pop(state.discoveries, discovery) do
      {nil, _} ->
        state

      {%{monitor: monitor}, discoveries} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])
        %{state | discoveries: discoveries, monitors: Map.delete(state.monitors, monitor)}
    end
  end

  defp normalize_unsubscribe(:ok), do: :ok
  defp normalize_unsubscribe({:error, :unknown_subscription}), do: :ok
  defp normalize_unsubscribe({:error, _reason} = error), do: error

  defp normalize_close(:ok), do: :ok
  defp normalize_close({:error, :closed}), do: :ok
  defp normalize_close({:error, _reason} = error), do: error

  defp put_if_present(options, _key, nil), do: options
  defp put_if_present(options, key, value), do: Keyword.put(options, key, value)
end
