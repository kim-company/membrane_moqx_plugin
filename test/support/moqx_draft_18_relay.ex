defmodule Membrane.MOQX.TestDraft18Relay do
  @moduledoc false

  alias MOQX.Protocol.MOQTDraft18.{Codec, SubgroupDecoder}
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

  def start_initialized_queue(namespace, media_track, repeat? \\ false) do
    mode = if repeat?, do: :initialized_queue_twice, else: :initialized_queue
    start_mode(namespace, media_track, mode)
  end

  defp start_mode(namespace, media_track, mode) do
    {:ok, network} = Support.start_network()
    parent = self()

    task =
      Task.async(fn ->
        relay(parent, network, namespace, media_track, mode)
      end)

    receive do
      {:draft18_relay_ready, port} ->
        %__MODULE__{
          task: task,
          network: network,
          endpoint: "moqt://localhost:#{port}"
        }
    after
      @timeout -> raise "draft-18 test relay did not start"
    end
  end

  def subscribe(%__MODULE__{task: task}) do
    send(task.pid, {:subscribe, self()})

    receive do
      {:draft18_subscribed, pid} when pid == task.pid -> :ok
      {:draft18_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :subscribe_timeout}
    end
  end

  def unsubscribe(%__MODULE__{task: task}) do
    send(task.pid, {:unsubscribe, self()})

    receive do
      {:draft18_unsubscribed, pid} when pid == task.pid -> :ok
      {:draft18_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :unsubscribe_timeout}
    end
  end

  def await_publisher_finish(%__MODULE__{task: task}, stream_count) do
    send(task.pid, {:await_publisher_finish, self(), stream_count})

    receive do
      {:draft18_publisher_finished, pid} when pid == task.pid -> :ok
      {:draft18_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
    after
      @timeout -> {:error, :publisher_finish_timeout}
    end
  end

  def transport(%__MODULE__{network: network}) do
    {Support, network: network, profile: :draft_18}
  end

  def await_pending(%__MODULE__{task: task}, name) do
    receive do
      {:draft18_track_pending, pid, ^name} when pid == task.pid -> :ok
    after
      @timeout ->
        case Task.yield(task, 0) do
          {:ok, result} -> {:error, {:relay_stopped_before_track_pending, name, result}}
          nil -> {:error, {:track_pending_timeout, name}}
        end
    end
  end

  def ready(%__MODULE__{task: task}, name), do: send(task.pid, {:ready, name})

  def capture(%__MODULE__{task: task}) do
    receive do
      {:draft18_capture, pid, capture} when pid == task.pid -> {:ok, capture}
      {:draft18_relay_error, pid, reason} when pid == task.pid -> {:error, reason}
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
      with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_18),
           {:ok, listener, ctx} <- Transport.listen(ctx, 0),
           {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
        send(parent, {:draft18_relay_ready, port})
        serve(parent, ctx, listener, port, namespace, media_name, mode)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} = error ->
        send(parent, {:draft18_relay_error, self(), reason})
        error

      {:error, reason, _ctx} ->
        send(parent, {:draft18_relay_error, self(), reason})
        {:error, reason}
    end
  end

  defp serve(parent, ctx, listener, port, namespace, media_name, mode) do
    initialized? = mode in [:initialized, :initialized_queue, :initialized_queue_twice]
    catalog_name = if initialized?, do: ".catalog", else: "catalog"
    catalog_ref = %MOQX.TrackRef{namespace: namespace, track: catalog_name}
    media_ref = %MOQX.TrackRef{namespace: namespace, track: media_name}

    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, client_setup, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- setup(ctx, conn, client_setup, port) do
      Process.put(:draft18_data_streams, [])
      Process.put(:draft18_request_streams, [])
      Process.put(:draft18_publish_streams, %{})

      with {:ok, publication, ctx} <- accept_request_stream(ctx, conn),
           {:ok, ctx} <- accept_publication(ctx, publication, namespace),
           {:ok, ctx} <- ready_track(parent, ctx, conn, 2, catalog_ref, 0) do
        # The fourth argument is the request-stream connection handle retained
        # by the mode helpers (it used to be the draft-16 shared control stream).
        serve_mode(parent, ctx, conn, conn, media_ref, mode)
      end
    end
  end

  defp serve_mode(parent, ctx, conn, control, media_ref, {:capture, delivery}) do
    with {:ok, ctx} <- ready_track(parent, ctx, control, 4, media_ref, 1) do
      capture_publication(parent, ctx, conn, delivery)
    end
  end

  defp serve_mode(parent, ctx, conn, control, media_ref, mode)
       when mode in [:initialized, :initialized_queue, :initialized_queue_twice] do
    init_ref = %{media_ref | track: media_ref.track <> ".init"}

    with {:ok, ctx} <- ready_track(parent, ctx, control, 4, init_ref, 1),
         {:ok, initialization, ctx} <- receive_subgroup(ctx, conn),
         {:ok, ctx} <- ready_track(parent, ctx, control, 6, media_ref, 2),
         {:ok, catalog, ctx} <- receive_subgroup(ctx, conn) do
      send(
        parent,
        {:draft18_capture, self(), %{initialization: initialization, catalog: catalog}}
      )

      with {:ok, ctx} <-
             ready_track(parent, ctx, control, 8, %{init_ref | track: init_ref.track <> ".1"}, 3),
           {:ok, next_initialization, ctx} <- receive_subgroup(ctx, conn),
           {:ok, next_catalog, ctx} <- receive_subgroup(ctx, conn) do
        send(
          parent,
          {:draft18_capture, self(),
           %{initialization: next_initialization, catalog: next_catalog}}
        )

        finish_initialized(parent, ctx, conn, control, init_ref, mode)
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

  defp finish_initialized(_parent, ctx, conn, _control, _ref, :initialized),
    do: await_stop(ctx, conn)

  defp finish_initialized(parent, ctx, conn, control, ref, mode) do
    {prefix, count, ctx} = queue_prefix(parent, ctx, conn, control, ref, mode)

    {objects, ctx} =
      Enum.map_reduce(1..count, ctx, fn _, ctx ->
        {:ok, object, ctx} = receive_subgroup(ctx, conn)
        {object, ctx}
      end)

    send(parent, {:draft18_capture, self(), prefix ++ objects})
    await_stop(ctx, conn)
  end

  defp queue_prefix(_parent, ctx, _conn, _control, _ref, :initialized_queue),
    do: {[], 5, ctx}

  defp queue_prefix(parent, ctx, conn, control, ref, :initialized_queue_twice) do
    {:ok, media, ctx} = receive_subgroup(ctx, conn)
    {:ok, ctx} = ready_track(parent, ctx, control, 10, %{ref | track: ref.track <> ".2"}, 4)
    {:ok, initialization, ctx} = receive_subgroup(ctx, conn)
    {[media, initialization], 4, ctx}
  end

  defp capture_publication(parent, ctx, conn, delivery) do
    with {:ok, catalog, datagrams, ctx} <- receive_subgroup_with_datagrams(ctx, conn),
         {:ok, media, ctx} <- receive_media(ctx, conn, delivery, datagrams),
         {:ok, refresh, ctx} <- receive_subgroup(ctx, conn) do
      send(parent, {
        :draft18_capture,
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

  defp controlled_subscription(ctx, conn, _control, media_ref, track_alias) do
    receive do
      {:subscribe, caller} ->
        subscribe = Codec.subscribe(1, media_ref, [])

        with {:ok, request, ctx} <-
               Transport.open_stream(ctx, conn, direction: :bidirectional),
             {:ok, _send, ctx} <- Transport.send_stream(ctx, request, subscribe),
             expected = Codec.subscribe_ok(track_alias, group_order: :ascending),
             {:ok, ^expected, ctx} <-
               Transport.recv_stream(ctx, request, byte_size(expected)) do
          send(caller, {:draft18_subscribed, self()})
          await_unsubscribe(ctx, conn, request, caller)
        end
    after
      @timeout -> {:error, :controlled_subscribe_timeout}
    end
  end

  defp await_unsubscribe(ctx, conn, control, _subscriber) do
    receive do
      {:unsubscribe, caller} ->
        with {:ok, ctx} <- Transport.abort_sending(ctx, control, 0x01) do
          send(caller, {:draft18_unsubscribed, self()})
          await_stop(ctx, conn)
        end

      {:await_publisher_finish, caller, stream_count} ->
        expected_track = Codec.publish_done(2, 1, "track ended")
        expected_subscription = Codec.publish_done(2, stream_count, "track ended")
        publish_stream = Map.fetch!(Process.get(:draft18_publish_streams), 1)

        with {:ok, ^expected_track, ctx} <-
               Transport.recv_stream(ctx, publish_stream, byte_size(expected_track)),
             {:ok, ^expected_subscription, ctx} <-
               Transport.recv_stream(ctx, control, byte_size(expected_subscription)) do
          send(caller, {:draft18_publisher_finished, self()})
          await_stop(ctx, conn)
        end
    after
      @timeout -> {:error, :controlled_unsubscribe_timeout}
    end
  end

  defp setup(ctx, conn, control, port) do
    expected = Codec.client_setup(URI.parse("moqt://localhost:#{port}"))

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         {:ok, server_setup, ctx} <-
           Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, server_setup, <<0xAF, 0, 0, 0>>) do
      {:ok, ctx}
    end
  end

  defp accept_publication(ctx, control, namespace) do
    expected = Codec.publish_namespace(0, namespace)

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, control, Codec.request_ok()) do
      {:ok, ctx}
    end
  end

  defp ready_track(parent, ctx, conn, request_id, track_ref, track_alias) do
    expected = Codec.publish_track(request_id, track_ref, track_alias)

    with {:ok, request, ctx} <- accept_request_stream(ctx, conn),
         {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, request, byte_size(expected)),
         :ok <- notify_and_wait(parent, track_ref.track),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, request, Codec.request_ok()) do
      Process.put(
        :draft18_publish_streams,
        Map.put(Process.get(:draft18_publish_streams, %{}), track_alias, request)
      )

      {:ok, ctx}
    end
  end

  # Request messages live on client-opened bidirectional streams in draft-18.
  # A publisher may race a subgroup stream with the next request, so retain
  # unidirectional streams for the data receiver instead of consuming them.
  defp accept_request_stream(ctx, conn) do
    case request_streams() do
      [stream | rest] ->
        Process.put(:draft18_request_streams, rest)
        {:ok, stream, ctx}

      [] ->
        case Transport.accept_stream(ctx, conn, [], @timeout) do
          {:ok, %{info: %{direction: :bidirectional}} = stream, ctx} ->
            {:ok, stream, ctx}

          {:ok, stream, ctx} ->
            Process.put(:draft18_data_streams, data_streams() ++ [stream])
            accept_request_stream(ctx, conn)

          other ->
            other
        end
    end
  end

  defp data_streams, do: Process.get(:draft18_data_streams, [])
  defp request_streams, do: Process.get(:draft18_request_streams, [])

  defp accept_data_stream(ctx, conn) do
    case data_streams() do
      [stream | rest] ->
        Process.put(:draft18_data_streams, rest)
        {:ok, stream, ctx}

      [] ->
        case Transport.accept_stream(ctx, conn, [], @timeout) do
          {:ok, %{info: %{direction: :unidirectional}} = stream, ctx} ->
            {:ok, stream, ctx}

          {:ok, stream, ctx} ->
            Process.put(:draft18_request_streams, request_streams() ++ [stream])
            accept_data_stream(ctx, conn)

          other ->
            other
        end
    end
  end

  defp notify_and_wait(parent, track_name) do
    send(parent, {:draft18_track_pending, self(), track_name})

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
           accept_data_stream(ctx, conn),
         {:ok, ctx} <- Transport.set_active(ctx, stream, true) do
      receive_stream_object(ctx, stream, %SubgroupDecoder{}, [])
    end
  end

  defp receive_stream_object(ctx, stream, decoder, datagrams) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:stream_data, ^stream, data, _metadata}, ctx} ->
        case SubgroupDecoder.push(decoder, data) do
          {:ok, _decoder, [object | _terminal_events]} ->
            {:ok, object, Enum.reverse(datagrams), ctx}

          {:ok, decoder, []} ->
            receive_stream_object(ctx, stream, decoder, datagrams)

          {:error, reason} ->
            {:error, reason, ctx}
        end

      {:ok, {:datagram, _conn, data, _metadata}, ctx} ->
        receive_stream_object(ctx, stream, decoder, [data | datagrams])

      {:ok, _event, ctx} ->
        receive_stream_object(ctx, stream, decoder, datagrams)

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
