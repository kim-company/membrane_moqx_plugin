defmodule Membrane.MOQX.TestLite05Publisher do
  @moduledoc false

  alias MOQX.Protocol.MOQLite05.Codec

  alias MOQX.Protocol.MOQLite05.Messages.{
    Frame,
    Group,
    Setup,
    Subscribe,
    SubscribeEnd,
    SubscribeOk,
    Track,
    TrackInfo
  }

  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 2_000
  @close_timeout 10_000

  defstruct [:task, :network, :endpoint]

  def start(%MOQX.TrackRef{} = track, timescale, frames) do
    {:ok, network} = Support.start_network()
    parent = self()
    task = Task.async(fn -> publish(parent, network, track, timescale, frames) end)

    receive do
      {:publisher_ready, port} ->
        %__MODULE__{task: task, network: network, endpoint: "moql://localhost:#{port}"}
    after
      @timeout -> raise "MoQ Lite 05 test publisher did not start"
    end
  end

  def transport(%__MODULE__{network: network}),
    do: {Support, network: network, profile: :moq_lite_05}

  def finish_group(%__MODULE__{task: task}), do: send(task.pid, :finish_group)

  def finish_subscription(%__MODULE__{task: task}), do: send(task.pid, :finish_subscription)

  def await_shutdown(%__MODULE__{task: task}) do
    case Task.yield(task, @timeout) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      nil -> {:error, :publisher_shutdown_timeout}
    end
  end

  defp publish(parent, network, track, timescale, frames) do
    with {:ok, ctx} <- Transport.new(Support, network: network, profile: :moq_lite_05),
         {:ok, listener, ctx} <- Transport.listen(ctx, 0),
         {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
      send(parent, {:publisher_ready, port})
      serve(ctx, listener, track, timescale, frames)
    end
  end

  defp serve(ctx, listener, track, timescale, frames) do
    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, setup_stream, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_setup(ctx, setup_stream),
         {:ok, track_stream, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, subscribe_stream, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_track(ctx, track_stream, track),
         {:ok, ctx} <- receive_subscribe(ctx, subscribe_stream, track),
         {:ok, ctx} <- send_track_info(ctx, track_stream, timescale),
         {:ok, ctx} <- send_subscription(ctx, subscribe_stream, conn, frames),
         {:ok, _event, _ctx} <- receive_connection_close(ctx, conn) do
      :ok
    end
  end

  defp receive_setup(ctx, stream) do
    bytes =
      IO.iodata_to_binary([
        MOQX.Codec.encode_varint(0x1),
        Codec.encode_setup(%Setup{path: "/", role: :both})
      ])

    expect_bytes(ctx, stream, bytes)
  end

  defp receive_track(ctx, stream, track) do
    bytes =
      IO.iodata_to_binary([
        MOQX.Codec.encode_varint(0x6),
        Codec.encode_track(%Track{
          broadcast_path: Enum.join(track.namespace, "/"),
          track_name: track.track
        })
      ])

    expect_bytes(ctx, stream, bytes)
  end

  defp receive_subscribe(ctx, stream, track) do
    bytes =
      IO.iodata_to_binary([
        MOQX.Codec.encode_varint(0x2),
        Codec.encode_subscribe(%Subscribe{
          subscribe_id: 0,
          broadcast_path: Enum.join(track.namespace, "/"),
          track_name: track.track,
          subscriber_priority: 128
        })
      ])

    expect_bytes(ctx, stream, bytes)
  end

  defp send_track_info(ctx, stream, timescale) do
    info = %TrackInfo{
      publisher_priority: 17,
      publisher_ordered: false,
      publisher_max_latency: 1_000,
      timescale: timescale
    }

    case Transport.send_stream(ctx, stream, Codec.encode_track_info(info), finish: true) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:track_info_send_failed, other}}
    end
  end

  defp send_subscription(ctx, subscribe_stream, conn, frames) do
    groups = if is_list(hd(frames)), do: frames, else: [frames]

    with {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             subscribe_stream,
             Codec.encode_subscribe_response(%SubscribeOk{group: 7})
           ),
         {:ok, group_stream, ctx} <- send_groups(ctx, conn, groups, 7),
         :ok <- await_message(:finish_group, :group_finish_timeout),
         {:ok, ctx} <- Transport.finish_sending(ctx, group_stream),
         :ok <- await_message(:finish_subscription, :finish_timeout),
         {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             subscribe_stream,
             Codec.encode_subscribe_response(%SubscribeEnd{group: 7 + length(groups)}),
             finish: true
           ) do
      {:ok, ctx}
    end
  end

  defp send_groups(ctx, conn, [frames | remaining], sequence) do
    with {:ok, stream, ctx} <- Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, stream, group_bytes(frames, sequence)) do
      continue_groups(ctx, conn, stream, remaining, sequence)
    end
  end

  defp continue_groups(ctx, _conn, stream, [], _sequence), do: {:ok, stream, ctx}

  defp continue_groups(ctx, conn, stream, remaining, sequence) do
    with {:ok, ctx} <- Transport.finish_sending(ctx, stream),
         do: send_groups(ctx, conn, remaining, sequence + 1)
  end

  defp await_message(message, timeout_reason) do
    receive do
      ^message -> :ok
    after
      @close_timeout -> {:error, timeout_reason}
    end
  end

  defp group_bytes(frames, sequence) do
    encoded_frames =
      Enum.map(frames, fn {timestamp_delta, payload} ->
        Codec.encode_frame(%Frame{timestamp_delta: timestamp_delta, payload: payload})
      end)

    IO.iodata_to_binary([
      MOQX.Codec.encode_varint(0x0),
      Codec.encode_group(%Group{subscribe_id: 0, group_sequence: sequence}),
      encoded_frames
    ])
  end

  defp expect_bytes(ctx, stream, expected) do
    case Transport.recv_stream(ctx, stream, byte_size(expected)) do
      {:ok, ^expected, ctx} -> {:ok, ctx}
      other -> {:error, {:unexpected_bytes, other}}
    end
  end

  defp receive_connection_close(ctx, conn) do
    case Transport.receive_event(ctx, @close_timeout) do
      {:ok, {:connection_event, ^conn, :closed, _metadata} = event, ctx} ->
        {:ok, event, ctx}

      {:ok, _event, ctx} ->
        receive_connection_close(ctx, conn)

      {:unknown, _message, ctx} ->
        receive_connection_close(ctx, conn)

      {:timeout, ctx} ->
        {:error, :connection_close_timeout, ctx}
    end
  end
end
