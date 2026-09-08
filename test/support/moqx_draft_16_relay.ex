defmodule Membrane.MOQX.TestDraft16Relay do
  @moduledoc false

  alias MOQX.Protocol.MOQTDraft16.{Codec, SubgroupDecoder}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 5_000

  defstruct [:task, :network, :endpoint]

  def start(namespace, media_track, delivery) do
    start_mode(namespace, media_track, {:capture, delivery})
  end

  def start_controlled(namespace, media_track) do
    start_mode(namespace, media_track, :controlled)
  end

  def start_reactive(namespace, media_track) do
    start_mode(namespace, media_track, :reactive)
  end

  def start_initialized(namespace, media_track) do
    start_mode(namespace, media_track, :initialized)
  end

  defp start_mode(namespace, media_track, mode) do
    {:ok, network} = Support.start_network()
    parent = self()

    task =
      Task.async(fn ->
        relay(parent, network, namespace, media_track, mode)
      end)

    receive do
      {:draft16_relay_ready, port} ->
        %__MODULE__{
          task: task,
          network: network,
          endpoint: "moqt://localhost:#{port}"
        }
    after
      @timeout -> raise "draft-16 test relay did not start"
    end
  end

  def subscribe(%__MODULE__{task: task}) do
    send(task.pid, {:subscribe, self()})

    receive do
      {:draft16_subscribed, pid} when pid == task.pid -> :ok
      {:draft16_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :subscribe_timeout}
    end
  end

  def unsubscribe(%__MODULE__{task: task}) do
    send(task.pid, {:unsubscribe, self()})

    receive do
      {:draft16_unsubscribed, pid} when pid == task.pid -> :ok
      {:draft16_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :unsubscribe_timeout}
    end
  end

  def await_publisher_finish(%__MODULE__{task: task}, stream_count) do
    send(task.pid, {:await_publisher_finish, self(), stream_count})

    receive do
      {:draft16_publisher_finished, pid} when pid == task.pid -> :ok
      {:draft16_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :publisher_finish_timeout}
    end
  end

  def transport(%__MODULE__{network: network}) do
    {Support, network: network, profile: :draft_16}
  end

  def await_pending(%__MODULE__{task: task}, name) do
    receive do
      {:draft16_track_pending, pid, ^name} when pid == task.pid -> :ok
    after
      @timeout -> {:error, {:track_pending_timeout, name}}
    end
  end

  def ready(%__MODULE__{task: task}, name), do: send(task.pid, {:ready, name})

  def capture(%__MODULE__{task: task}) do
    receive do
      {:draft16_capture, pid, capture} when pid == task.pid -> {:ok, capture}
      {:draft16_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :capture_timeout}
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

  defp relay(parent, network, namespace, media_name, mode) do
    result =
      with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_16),
           {:ok, listener, ctx} <- Transport.listen(ctx, 0),
           {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
        send(parent, {:draft16_relay_ready, port})
        serve(parent, ctx, listener, port, namespace, media_name, mode)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} = error ->
        send(parent, {:draft16_relay_error, self(), reason})
        error

      {:error, reason, _ctx} ->
        send(parent, {:draft16_relay_error, self(), reason})
        {:error, reason}
    end
  end

  defp serve(parent, ctx, listener, port, namespace, media_name, mode) do
    catalog_name = if mode == :initialized, do: ".catalog", else: "catalog"
    catalog_ref = %MOQX.TrackRef{namespace: namespace, track: catalog_name}
    media_ref = %MOQX.TrackRef{namespace: namespace, track: media_name}

    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, control, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- setup(ctx, control, port, if(mode == :initialized, do: 16, else: 4)),
         {:ok, ctx} <- accept_publication(ctx, control, namespace),
         {:ok, ctx} <- ready_track(parent, ctx, control, 2, catalog_ref, 0) do
      serve_mode(parent, ctx, conn, control, media_ref, mode)
    end
  end

  defp serve_mode(parent, ctx, conn, control, media_ref, {:capture, delivery}) do
    with {:ok, ctx} <- ready_track(parent, ctx, control, 4, media_ref, 1) do
      capture_publication(parent, ctx, conn, delivery)
    end
  end

  defp serve_mode(parent, ctx, conn, control, media_ref, :initialized) do
    init_ref = %{media_ref | track: media_ref.track <> ".init"}

    with {:ok, ctx} <- ready_track(parent, ctx, control, 4, init_ref, 1),
         {:ok, initialization, ctx} <- receive_subgroup(ctx, conn),
         {:ok, ctx} <- ready_track(parent, ctx, control, 6, media_ref, 2),
         {:ok, catalog, ctx} <- receive_subgroup(ctx, conn) do
      send(
        parent,
        {:draft16_capture, self(), %{initialization: initialization, catalog: catalog}}
      )

      with {:ok, ctx} <-
             ready_track(parent, ctx, control, 8, %{init_ref | track: init_ref.track <> ".1"}, 3),
           {:ok, next_initialization, ctx} <- receive_subgroup(ctx, conn),
           {:ok, next_catalog, ctx} <- receive_subgroup(ctx, conn) do
        send(
          parent,
          {:draft16_capture, self(),
           %{initialization: next_initialization, catalog: next_catalog}}
        )

        await_stop(ctx, conn)
      end
    end
  end

  defp serve_mode(parent, ctx, conn, control, media_ref, :controlled) do
    with {:ok, ctx} <- ready_track(parent, ctx, control, 4, media_ref, 1) do
      controlled_subscription(ctx, conn, control, media_ref, 2)
    end
  end

  defp serve_mode(_parent, ctx, conn, control, media_ref, :reactive) do
    controlled_subscription(ctx, conn, control, media_ref, 1)
  end

  defp capture_publication(parent, ctx, conn, delivery) do
    with {:ok, catalog, datagrams, ctx} <- receive_subgroup_with_datagrams(ctx, conn),
         {:ok, media, ctx} <- receive_media(ctx, conn, delivery, datagrams),
         {:ok, refresh, ctx} <- receive_subgroup(ctx, conn) do
      send(parent, {
        :draft16_capture,
        self(),
        %{catalog: catalog, media: media, refresh: refresh}
      })

      await_stop(ctx, conn)
    end
  end

  defp receive_media(ctx, _conn, :datagram, [data | _rest]) do
    case Codec.decode_datagram(data) do
      {:ok, object} -> {:ok, object, ctx}
      {:error, reason} -> {:error, reason, ctx}
    end
  end

  defp receive_media(ctx, conn, delivery, []), do: receive_media(ctx, conn, delivery)

  defp controlled_subscription(ctx, conn, control, media_ref, track_alias) do
    receive do
      {:subscribe, caller} ->
        subscribe = Codec.subscribe(1, media_ref, [])

        with {:ok, _send, ctx} <- Transport.send_stream(ctx, control, subscribe),
             expected = Codec.subscribe_ok(1, track_alias, group_order: :ascending),
             {:ok, ^expected, ctx} <-
               Transport.recv_stream(ctx, control, byte_size(expected)) do
          send(caller, {:draft16_subscribed, self()})
          await_unsubscribe(ctx, conn, control, caller)
        end
    after
      @timeout -> {:error, :controlled_subscribe_timeout}
    end
  end

  defp await_unsubscribe(ctx, conn, control, _subscriber) do
    receive do
      {:unsubscribe, caller} ->
        unsubscribe = Codec.unsubscribe(1)
        expected = Codec.publish_done(1, 3, 0, "subscription ended")

        with {:ok, _send, ctx} <- Transport.send_stream(ctx, control, unsubscribe),
             {:ok, ^expected, ctx} <-
               Transport.recv_stream(ctx, control, byte_size(expected)) do
          send(caller, {:draft16_unsubscribed, self()})
          await_stop(ctx, conn)
        end

      {:await_publisher_finish, caller, stream_count} ->
        expected_track = Codec.publish_done(4, 2, 1, "track ended")
        expected_subscription = Codec.publish_done(1, 2, stream_count, "track ended")

        with {:ok, ^expected_track, ctx} <-
               Transport.recv_stream(ctx, control, byte_size(expected_track)),
             {:ok, ^expected_subscription, ctx} <-
               Transport.recv_stream(ctx, control, byte_size(expected_subscription)) do
          send(caller, {:draft16_publisher_finished, self()})
          await_stop(ctx, conn)
        end
    after
      @timeout -> {:error, :controlled_unsubscribe_timeout}
    end
  end

  defp setup(ctx, control, port, max_request_id) do
    expected = Codec.client_setup(URI.parse("moqt://localhost:#{port}"))

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, control, <<0x21, 0, 3, 1, 2, max_request_id>>) do
      {:ok, ctx}
    end
  end

  defp accept_publication(ctx, control, namespace) do
    expected = Codec.publish_namespace(0, namespace)

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, control, <<0x07, 0, 2, 0, 0>>) do
      {:ok, ctx}
    end
  end

  defp ready_track(parent, ctx, control, request_id, track_ref, track_alias) do
    expected = Codec.publish_track(request_id, track_ref, track_alias)

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         :ok <- notify_and_wait(parent, track_ref.track),
         {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             control,
             <<0x1E, 0, 2, request_id, 0>>
           ) do
      {:ok, ctx}
    end
  end

  defp notify_and_wait(parent, track_name) do
    send(parent, {:draft16_track_pending, self(), track_name})

    receive do
      {:ready, ^track_name} -> :ok
    after
      @timeout -> {:error, {:track_readiness_timeout, track_name}}
    end
  end

  defp receive_media(ctx, conn, :subgroup), do: receive_subgroup(ctx, conn)

  defp receive_media(ctx, conn, :datagram) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:datagram, ^conn, data, _metadata}, ctx} ->
        case Codec.decode_datagram(data) do
          {:ok, object} -> {:ok, object, ctx}
          {:error, reason} -> {:error, reason, ctx}
        end

      {:ok, _event, ctx} ->
        receive_media(ctx, conn, :datagram)

      other ->
        other
    end
  end

  defp receive_subgroup(ctx, conn) do
    with {:ok, object, _datagrams, ctx} <- receive_subgroup_with_datagrams(ctx, conn),
         do: {:ok, object, ctx}
  end

  defp receive_subgroup_with_datagrams(ctx, conn) do
    with {:ok, stream, ctx} <-
           Transport.accept_stream(ctx, conn, [active: true], @timeout),
         {:ok, ctx} <- Transport.set_active(ctx, stream, true),
         {:ok, bytes, datagrams, ctx} <- receive_stream_data(ctx, stream, []),
         {:ok, _decoder, [object]} <- SubgroupDecoder.push(%SubgroupDecoder{}, bytes) do
      {:ok, object, datagrams, ctx}
    end
  end

  defp receive_stream_data(ctx, stream, datagrams) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:stream_data, ^stream, data, _metadata}, ctx} ->
        {:ok, data, Enum.reverse(datagrams), ctx}

      {:ok, {:datagram, _conn, data, _metadata}, ctx} ->
        receive_stream_data(ctx, stream, [data | datagrams])

      {:ok, _event, ctx} ->
        receive_stream_data(ctx, stream, datagrams)

      other ->
        other
    end
  end

  defp await_stop(ctx, conn) do
    receive do
      :stop ->
        case Transport.close_connection(ctx, conn, 0) do
          {:ok, _ctx} -> :ok
          {:error, reason, _ctx} -> {:error, reason}
        end
    after
      @timeout -> {:error, :stop_timeout}
    end
  end
end
