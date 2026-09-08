defmodule Membrane.MOQX.TestLite05Relay do
  @moduledoc false

  alias MOQX.Protocol.MOQLite05.Codec

  alias MOQX.Protocol.MOQLite05.Messages.{
    AnnounceBroadcast,
    AnnounceOk,
    AnnounceRequest,
    Frame,
    Group,
    Setup,
    Subscribe,
    SubscribeEnd,
    SubscribeOk,
    Track
  }

  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 2_000

  defstruct [:task, :network, :endpoint]

  def start(namespace, track_name, track_info, frames, options \\ []) do
    {:ok, network} = Support.start_network()
    parent = self()

    task =
      Task.async(fn ->
        relay(parent, network, namespace, track_name, track_info, frames, options)
      end)

    receive do
      {:lite_relay_ready, port} ->
        %__MODULE__{task: task, network: network, endpoint: "moql://localhost:#{port}"}
    after
      @timeout -> raise "MoQ Lite 05 test relay did not start"
    end
  end

  def transport(%__MODULE__{network: network}),
    do: {Support, network: network, profile: :moq_lite_05}

  def subscribe(%__MODULE__{task: task}) do
    send(task.pid, :subscribe)

    receive do
      {:lite_subscribed, pid, track_info} when pid == task.pid -> {:ok, track_info}
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :subscribe_timeout}
    end
  end

  def request_subscription(%__MODULE__{task: task}) do
    send(task.pid, :subscribe)

    receive do
      {:lite_request_sent, pid} when pid == task.pid -> :ok
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :request_timeout}
    end
  end

  def request_track_info(%__MODULE__{task: task}) do
    send(task.pid, :request_track)

    receive do
      {:lite_track_info, pid, track_info} when pid == task.pid -> {:ok, track_info}
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :track_info_timeout}
    end
  end

  def capture(%__MODULE__{task: task}) do
    receive do
      {:lite_capture, pid, capture} when pid == task.pid -> {:ok, capture}
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :capture_timeout}
    end
  end

  def unsubscribe(%__MODULE__{task: task}) do
    send(task.pid, {:unsubscribe, self()})

    receive do
      {:lite_unsubscribed, pid} when pid == task.pid -> :ok
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :unsubscribe_timeout}
    end
  end

  def await_publisher_finish(%__MODULE__{task: task}) do
    receive do
      {:lite_publisher_finished, pid} when pid == task.pid -> :ok
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :publisher_finish_timeout}
    end
  end

  def assert_track_withdrawn(%__MODULE__{task: task}) do
    send(task.pid, {:assert_track_withdrawn, self()})

    receive do
      {:lite_track_withdrawn, pid} when pid == task.pid -> :ok
      {:lite_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :track_withdrawal_timeout}
    end
  end

  def await_shutdown(%__MODULE__{task: task}) do
    send(task.pid, :stop)

    case Task.yield(task, @timeout) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      nil -> {:error, :relay_shutdown_timeout}
    end
  end

  defp relay(parent, network, namespace, track_name, track_info, frames, options) do
    result =
      with {:ok, ctx} <- Transport.new(Support, network: network, profile: :moq_lite_05),
           {:ok, listener, ctx} <- Transport.listen(ctx, 0),
           {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
        send(parent, {:lite_relay_ready, port})
        serve(parent, ctx, listener, namespace, track_name, track_info, frames, options)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} = error ->
        send(parent, {:lite_relay_error, self(), reason})
        error

      {:error, reason, _ctx} ->
        send(parent, {:lite_relay_error, self(), reason})
        {:error, reason}
    end
  end

  defp serve(parent, ctx, listener, namespace, track_name, track_info, frames, options) do
    path = Enum.join(namespace, "/")

    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, setup, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_setup(ctx, setup),
         :ok <- await_subscribe(),
         {:ok, announce, ctx} <- Transport.open_stream(ctx, conn, direction: :bidirectional),
         {:ok, ctx} <- announce(ctx, announce, path),
         {:ok, subscribe, ctx} <-
           begin_subscription(parent, ctx, conn, path, track_name, track_info, options[:mode]),
         {:ok, ctx} <- receive_subscription_ok(ctx, subscribe, Keyword.get(options, :group, 0)),
         {:ok, capture, ctx} <- receive_group(ctx, conn, frames, Keyword.get(options, :group, 0)),
         _message = send(parent, {:lite_capture, self(), capture}),
         {:ok, ctx} <- finish_subscription(parent, ctx, subscribe, options[:completion]),
         {:ok, ctx} <- verify_track_withdrawal(ctx, conn, path, track_name, options),
         :ok <- await_stop(),
         {:ok, _ctx} <- Transport.close_connection(ctx, conn, 0) do
      :ok
    end
  end

  defp begin_subscription(parent, ctx, conn, path, track_name, track_info, :controlled_reactive) do
    with {:ok, subscribe, ctx} <- Transport.open_stream(ctx, conn, direction: :bidirectional),
         {:ok, ctx} <- request_subscription(ctx, subscribe, path, track_name),
         _message = send(parent, {:lite_request_sent, self()}),
         :ok <- await_request_track(),
         {:ok, track, ctx} <- Transport.open_stream(ctx, conn, direction: :bidirectional),
         {:ok, ^track_info, ctx} <- request_track(ctx, track, path, track_name, track_info) do
      send(parent, {:lite_track_info, self(), track_info})
      {:ok, subscribe, ctx}
    end
  end

  defp begin_subscription(parent, ctx, conn, path, track_name, track_info, _mode) do
    with {:ok, track, ctx} <- Transport.open_stream(ctx, conn, direction: :bidirectional),
         {:ok, ^track_info, ctx} <- request_track(ctx, track, path, track_name, track_info),
         {:ok, subscribe, ctx} <- Transport.open_stream(ctx, conn, direction: :bidirectional),
         {:ok, ctx} <- request_subscription(ctx, subscribe, path, track_name) do
      send(parent, {:lite_subscribed, self(), track_info})
      {:ok, subscribe, ctx}
    end
  end

  defp finish_subscription(parent, ctx, stream, :publisher) do
    with {:ok, ctx} <-
           expect_bytes(ctx, stream, Codec.encode_subscribe_response(%SubscribeEnd{group: 1})) do
      send(parent, {:lite_publisher_finished, self()})
      {:ok, ctx}
    end
  end

  defp finish_subscription(_parent, ctx, stream, _completion), do: unsubscribe(ctx, stream)

  defp verify_track_withdrawal(ctx, conn, path, track_name, options) do
    if options[:verify_withdrawal] do
      receive do
        {:assert_track_withdrawn, caller} ->
          with {:ok, track, ctx} <-
                 Transport.open_stream(ctx, conn, direction: :bidirectional),
               {:ok, _send, ctx} <-
                 Transport.send_stream(
                   ctx,
                   track,
                   <<6,
                     Codec.encode_track(%Track{
                       broadcast_path: path,
                       track_name: track_name
                     })::binary>>
                 ),
               {:ok, ctx} <- expect_stream_abort(ctx, track, 0x10) do
            send(caller, {:lite_track_withdrawn, self()})
            {:ok, ctx}
          end
      after
        @timeout -> {:error, :track_withdrawal_not_checked, ctx}
      end
    else
      {:ok, ctx}
    end
  end

  defp expect_stream_abort(ctx, stream, error_code) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:stream_event, ^stream, :peer_aborted_sending, %{error_code: ^error_code}}, ctx} ->
        {:ok, ctx}

      {:ok, _event, ctx} ->
        expect_stream_abort(ctx, stream, error_code)

      {:unknown, _message, ctx} ->
        expect_stream_abort(ctx, stream, error_code)

      {:timeout, ctx} ->
        {:error, :track_withdrawal_not_observed, ctx}
    end
  end

  defp receive_setup(ctx, stream) do
    expected = <<1, Codec.encode_setup(%Setup{path: "/", role: :both})::binary>>
    expect_bytes(ctx, stream, expected)
  end

  defp announce(ctx, stream, path) do
    request =
      <<1, Codec.encode_announce_request(%AnnounceRequest{broadcast_path_prefix: ""})::binary>>

    expected =
      IO.iodata_to_binary([
        Codec.encode_announce_ok(%AnnounceOk{hop_id: 0, active_count: 1}),
        Codec.encode_announce_broadcast(%AnnounceBroadcast{
          status: :active,
          path_suffix: path,
          hop_ids: []
        })
      ])

    case Transport.send_stream(ctx, stream, request) do
      {:ok, _send, ctx} -> expect_bytes(ctx, stream, expected)
      other -> other
    end
  end

  defp request_track(ctx, stream, path, track_name, expected_info) do
    request =
      <<6, Codec.encode_track(%Track{broadcast_path: path, track_name: track_name})::binary>>

    expected = Codec.encode_track_info(expected_info)

    with {:ok, _send, ctx} <- Transport.send_stream(ctx, stream, request),
         {:ok, bytes, ctx} <- Transport.recv_stream(ctx, stream, byte_size(expected)),
         {:ok, info} <- Codec.decode_track_info(bytes) do
      {:ok, info, ctx}
    end
  end

  defp request_subscription(ctx, stream, path, track_name) do
    subscribe = %Subscribe{
      subscribe_id: 42,
      broadcast_path: path,
      track_name: track_name,
      subscriber_priority: 9
    }

    request = <<2, Codec.encode_subscribe(subscribe)::binary>>

    case Transport.send_stream(ctx, stream, request) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> other
    end
  end

  defp receive_subscription_ok(ctx, stream, group) do
    expect_bytes(ctx, stream, Codec.encode_subscribe_response(%SubscribeOk{group: group}))
  end

  defp receive_group(ctx, conn, frames, group) do
    expected =
      IO.iodata_to_binary([
        <<0>>,
        Codec.encode_group(%Group{subscribe_id: 42, group_sequence: group}),
        Enum.map(frames, fn {timestamp_delta, payload} ->
          Codec.encode_frame(%Frame{timestamp_delta: timestamp_delta, payload: payload})
        end)
      ])

    with {:ok, stream, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- expect_bytes(ctx, stream, expected) do
      {:ok, %{bytes: expected, frames: frames}, ctx}
    end
  end

  defp unsubscribe(ctx, stream) do
    receive do
      {:unsubscribe, caller} ->
        case Transport.finish_sending(ctx, stream) do
          {:ok, ctx} ->
            send(caller, {:lite_unsubscribed, self()})
            {:ok, ctx}

          other ->
            other
        end
    after
      @timeout -> {:error, :unsubscribe_not_requested, ctx}
    end
  end

  defp expect_bytes(ctx, stream, expected) do
    case Transport.recv_stream(ctx, stream, byte_size(expected)) do
      {:ok, ^expected, ctx} -> {:ok, ctx}
      other -> {:error, {:unexpected_bytes, other}}
    end
  end

  defp await_subscribe do
    receive do
      :subscribe -> :ok
    after
      @timeout -> {:error, :subscription_not_requested}
    end
  end

  defp await_request_track do
    receive do
      :request_track -> :ok
    after
      @timeout -> {:error, :track_not_requested}
    end
  end

  defp await_stop do
    receive do
      :stop -> :ok
    after
      @timeout -> {:error, :stop_timeout}
    end
  end
end
