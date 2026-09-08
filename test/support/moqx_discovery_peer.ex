defmodule Membrane.MOQX.TestDiscoveryPeer do
  @moduledoc false
  alias MOQX.Protocol.MOQLite05.{Codec, Messages}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  def start(prefix) do
    {:ok, network} = Support.start_network()
    parent = self()

    task =
      Task.async(fn ->
        {:ok, ctx} = Transport.new(Support, network: network, profile: :moq_lite_05)
        {:ok, listener, ctx} = Transport.listen(ctx, 0)
        {:ok, {_ip, port}} = Transport.local_address(ctx, listener)
        send(parent, {:discovery_listening, port})
        {:ok, conn, ctx} = Transport.accept(ctx, listener, [], 2_000)
        {:ok, conn, ctx} = Transport.handshake(ctx, conn, 2_000)
        {:ok, setup, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)
        setup_bytes = <<1, Codec.encode_setup(%Messages.Setup{path: "/", role: :both})::binary>>
        {:ok, ^setup_bytes, ctx} = Transport.recv_stream(ctx, setup, byte_size(setup_bytes))

        {streams, ctx} =
          Enum.reduce(List.wrap(prefix), {%{}, ctx}, fn prefix, {streams, ctx} ->
            {:ok, announce, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)

            request =
              <<1,
                Codec.encode_announce_request(%Messages.AnnounceRequest{
                  broadcast_path_prefix: prefix
                })::binary>>

            {:ok, ^request, ctx} = Transport.recv_stream(ctx, announce, byte_size(request))

            bytes = [
              Codec.encode_announce_ok(%Messages.AnnounceOk{hop_id: 0, active_count: 1}),
              broadcast(:active, "alice.hang")
            ]

            {:ok, _, ctx} = Transport.send_stream(ctx, announce, bytes)
            {Map.put(streams, announce, prefix), ctx}
          end)

        loop(ctx, conn, streams, parent)
      end)

    receive do
      {:discovery_listening, port} ->
        %{
          task: task,
          endpoint: "moql://localhost:#{port}",
          transport: {Support, network: network, profile: :moq_lite_05}
        }
    after
      2_000 -> raise "discovery peer startup timed out"
    end
  end

  def announce(peer, status, suffix), do: send(peer.task.pid, {:announce, status, suffix})

  defp broadcast(status, suffix) do
    Codec.encode_announce_broadcast(%Messages.AnnounceBroadcast{
      status: status,
      path_suffix: suffix,
      hop_ids: []
    })
  end

  defp loop(ctx, conn, streams, parent) do
    case Transport.receive_event(ctx, 2_000) do
      {:ok, {:connection_event, ^conn, :closed, _}, _ctx} ->
        :ok

      {:ok, {:stream_event, stream, :peer_aborted_receiving, _}, ctx} ->
        {prefix, streams} = Map.pop(streams, stream)
        if prefix, do: send(parent, {:discovery_cancelled, prefix})
        loop(ctx, conn, streams, parent)

      {:ok, _event, ctx} ->
        loop(ctx, conn, streams, parent)

      {:unknown, {:announce, status, suffix}, ctx} ->
        ctx =
          Enum.reduce(streams, ctx, fn {stream, _prefix}, ctx ->
            {:ok, _, ctx} = Transport.send_stream(ctx, stream, broadcast(status, suffix))
            ctx
          end)

        loop(ctx, conn, streams, parent)

      {:unknown, _message, ctx} ->
        loop(ctx, conn, streams, parent)

      {:timeout, _ctx} ->
        raise "discovery peer shutdown timed out"
    end
  end
end
