defmodule Membrane.MOQX.TestLiteBridge do
  @moduledoc false
  # A scoped semantic relay fixture, not a production relay. Two independent
  # server-side transport owners route actual client requests and groups.
  # Subscription IDs are translated; media bytes, timing deltas and FIN/RESET
  # remain the publisher's. No fixture-generated media or delivery grace delay.
  alias MOQX.Protocol.MOQLite05.{Codec, Messages}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  def start do
    {:ok, network} = Support.start_network()
    parent = self()
    upstream = Task.async(fn -> listen(parent, network, :upstream, nil) end)
    publisher_endpoint = endpoint(upstream)
    downstream = Task.async(fn -> listen(parent, network, :downstream, upstream.pid) end)

    %{
      upstream: upstream,
      downstream: downstream,
      publisher_endpoint: publisher_endpoint,
      subscriber_endpoint: endpoint(downstream),
      transport: {Support, network: network, profile: :moq_lite_05}
    }
  end

  def stop(relay) do
    for task <- [relay.downstream, relay.upstream] do
      send(task.pid, :stop)
      :ok = Task.await(task, 2_000)
    end

    :ok
  end

  defp endpoint(task) do
    receive do
      {:bridge_listening, pid, port} when pid == task.pid -> "moql://localhost:#{port}"
    after
      2_000 -> raise "Lite bridge failed to listen"
    end
  end

  defp listen(parent, network, role, upstream) do
    {:ok, ctx} = Transport.new(Support, network: network, profile: :moq_lite_05)
    {:ok, listener, ctx} = Transport.listen(ctx, 0)
    {:ok, {_ip, port}} = Transport.local_address(ctx, listener)
    send(parent, {:bridge_listening, self(), port})
    {:ok, conn, ctx} = Transport.accept(ctx, listener, [], 5_000)
    {:ok, conn, ctx} = Transport.handshake(ctx, conn, 2_000)
    {:ok, setup, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)
    bytes = <<1, Codec.encode_setup(%Messages.Setup{path: "/", role: :both})::binary>>
    {:ok, ^bytes, ctx} = Transport.recv_stream(ctx, setup, byte_size(bytes))

    loop(%{
      ctx: ctx,
      conn: conn,
      listener: listener,
      role: role,
      upstream: upstream,
      routes: %{},
      reverse: %{},
      headers: %{},
      subscriptions: %{},
      next_id: 10,
      deadline: System.monotonic_time(:millisecond) + 10_000,
      closed?: false
    })
  end

  defp loop(state) do
    # This backend exposes passive accept, not automatic peer-stream adoption.
    # Poll accept alongside active stream events; this is not a media/EOS gate.
    state = accept_pending(state)

    receive do
      :stop ->
        unless state.closed?, do: Transport.close_connection(state.ctx, state.conn, 0)
        # MOQX.Testing.Transport 0.9 has no listener-close callback.
        {:error, :unsupported, _ctx} = Transport.close_listener(state.ctx, state.listener)
        :ok

      message ->
        message |> handle(state) |> loop()
    after
      1 ->
        if System.monotonic_time(:millisecond) > state.deadline,
          do: raise("Lite bridge deadline exceeded"),
          else: loop(state)
    end
  end

  defp accept_pending(%{closed?: true} = state), do: state

  defp accept_pending(state) do
    case Transport.accept_stream(state.ctx, state.conn, [], 1) do
      {:ok, stream, ctx} ->
        {:ok, ctx} = Transport.set_active(ctx, stream, true)
        %{state | ctx: ctx, headers: Map.put_new(state.headers, stream, <<>>)}

      {:error, :timeout, ctx} ->
        %{state | ctx: ctx}
    end
  end

  defp handle({:request, peer, key, type, request}, state) do
    {request, state} = subscription_identity(type, request, peer, state)
    {:ok, stream, ctx} = Transport.open_stream(state.ctx, state.conn, direction: :bidirectional)
    {:ok, ctx} = Transport.set_active(ctx, stream, true)
    payload = if type == 2, do: Codec.encode_subscribe(request), else: Codec.encode_track(request)
    {:ok, _, ctx} = Transport.send_stream(ctx, stream, <<type, payload::binary>>)

    %{
      state
      | ctx: ctx,
        routes: Map.put(state.routes, stream, {:control, peer, key}),
        reverse: Map.put(state.reverse, {peer, key}, stream)
    }
  end

  defp handle({:relay_data, key, bytes}, state) do
    {:ok, _, ctx} = Transport.send_stream(state.ctx, key, bytes)
    %{state | ctx: ctx}
  end

  defp handle({:relay_group, key, group, bytes}, state) do
    {:ok, stream, ctx} = Transport.open_stream(state.ctx, state.conn, direction: :unidirectional)
    {:ok, _, ctx} = Transport.send_stream(ctx, stream, [<<0>>, Codec.encode_group(group), bytes])
    %{state | ctx: ctx, reverse: Map.put(state.reverse, key, stream)}
  end

  defp handle({:group_data, key, bytes}, state) do
    stream = Map.fetch!(state.reverse, key)
    {:ok, _, ctx} = Transport.send_stream(state.ctx, stream, bytes)
    %{state | ctx: ctx}
  end

  defp handle({:relay_event, key, event, metadata}, state) do
    stream = Map.get(state.reverse, key, key)
    forward_event(state, stream, event, metadata)
  end

  defp handle({:cancel, peer, key, event, metadata}, state) do
    case Map.fetch(state.reverse, {peer, key}) do
      {:ok, stream} -> forward_event(state, stream, event, metadata)
      :error -> state
    end
  end

  defp handle({:request_data, peer, key, bytes}, state) do
    stream = Map.fetch!(state.reverse, {peer, key})
    {:ok, _, ctx} = Transport.send_stream(state.ctx, stream, bytes)
    %{state | ctx: ctx}
  end

  defp handle(message, state) do
    case Transport.normalize_event(state.ctx, state.conn, message) do
      {:ok, event, ctx} -> transport_event(event, %{state | ctx: ctx})
      {:unknown, _message, ctx} -> %{state | ctx: ctx}
      {:error, reason, _ctx} -> raise "Lite bridge transport error: #{inspect(reason)}"
    end
  end

  defp subscription_identity(2, request, peer, state) do
    id = state.next_id
    subscriptions = Map.put(state.subscriptions, id, {peer, request.subscribe_id})
    {%{request | subscribe_id: id}, %{state | next_id: id + 1, subscriptions: subscriptions}}
  end

  defp subscription_identity(6, request, _peer, state), do: {request, state}

  defp transport_event({:stream_event, stream, :new_stream, _metadata}, state) do
    {:ok, ctx} = Transport.set_active(state.ctx, stream, true)
    %{state | ctx: ctx, headers: Map.put_new(state.headers, stream, <<>>)}
  end

  # Empty sends in the in-memory backend can surface as zero-byte data before
  # FIN. They contain no protocol message; relay the separate FIN event only.
  defp transport_event({:stream_data, _stream, <<>>, _metadata}, state), do: state

  defp transport_event({:stream_data, stream, bytes, _metadata}, state) do
    case Map.fetch(state.routes, stream) do
      {:ok, {:control, peer, key}} ->
        send(peer, {:relay_data, key, bytes})
        state

      {:ok, {:group, peer, key}} ->
        send(peer, {:group_data, key, bytes})
        state

      {:ok, :request} ->
        send(state.upstream, {:request_data, self(), stream, bytes})
        state

      :error ->
        header(state, stream, Map.get(state.headers, stream, <<>>) <> bytes)
    end
  end

  defp transport_event({:stream_event, stream, event, metadata}, state)
       when event in [:peer_finished_sending, :peer_aborted_sending, :peer_aborted_receiving] do
    case Map.get(state.routes, stream) do
      {kind, peer, key} when kind in [:control, :group] ->
        send(peer, {:relay_event, key, event, metadata})

      :request ->
        send(state.upstream, {:cancel, self(), stream, event, metadata})

      nil ->
        :ok
    end

    state
  end

  defp transport_event({:connection_event, _conn, :closed, _metadata}, state),
    do: %{state | closed?: true}

  defp transport_event(_event, state), do: state

  defp header(state, stream, bytes) do
    with {:ok, type, rest} <- MOQX.Codec.decode_varint(bytes),
         {:ok, length, payload} <- MOQX.Codec.decode_varint(rest),
         true <- byte_size(payload) >= length do
      prefix_length = byte_size(rest) - byte_size(payload)
      frame_length = prefix_length + length
      <<framed::binary-size(^frame_length), trailing::binary>> = rest
      request(state, stream, type, framed, trailing)
    else
      _incomplete -> %{state | headers: Map.put(state.headers, stream, bytes)}
    end
  end

  defp request(%{role: :downstream} = state, stream, type, framed, <<>>)
       when type in [2, 6] do
    {:ok, request} =
      if type == 2, do: Codec.decode_subscribe(framed), else: Codec.decode_track(framed)

    send(state.upstream, {:request, self(), stream, type, request})

    %{
      state
      | routes: Map.put(state.routes, stream, :request),
        headers: Map.delete(state.headers, stream)
    }
  end

  defp request(%{role: :upstream} = state, stream, 0, framed, trailing) do
    {:ok, group} = Codec.decode_group(framed)
    {peer, original_id} = Map.fetch!(state.subscriptions, group.subscribe_id)
    send(peer, {:relay_group, stream, %{group | subscribe_id: original_id}, trailing})

    %{
      state
      | routes: Map.put(state.routes, stream, {:group, peer, stream}),
        headers: Map.delete(state.headers, stream)
    }
  end

  defp forward_event(state, stream, event, metadata) do
    result =
      case event do
        :peer_finished_sending ->
          Transport.finish_sending(state.ctx, stream)

        :peer_aborted_sending ->
          Transport.abort_sending(state.ctx, stream, metadata.error_code)

        :peer_aborted_receiving ->
          Transport.abort_receiving(state.ctx, stream, metadata.error_code)
      end

    case result do
      {:ok, ctx} -> %{state | ctx: ctx}
      {:error, :send_side_finished, ctx} -> %{state | ctx: ctx}
      {:error, reason, _ctx} -> raise "Lite bridge forwarding failed: #{inspect(reason)}"
    end
  end
end
