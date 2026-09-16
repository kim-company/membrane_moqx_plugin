defmodule Membrane.MOQX.TestDraft18Publisher do
  @moduledoc false

  alias MOQX.Protocol.MOQTDraft18.Codec
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 2_000
  @close_timeout 10_000

  defstruct [:task, :network, :endpoint]

  def start(namespace, catalog, track_name, payload, opts \\ []) do
    {:ok, network} = Support.start_network()
    parent = self()

    config = %{
      namespace: namespace,
      catalog: catalog,
      track_name: track_name,
      payload: payload,
      opts: opts
    }

    task =
      Task.async(fn ->
        publish(parent, network, config)
      end)

    receive do
      {:publisher_ready, port} ->
        %__MODULE__{
          task: task,
          network: network,
          endpoint: "moqt://localhost:#{port}"
        }
    after
      @timeout -> raise "draft-18 test publisher did not start"
    end
  end

  def transport(%__MODULE__{network: network}) do
    {Support, network: network, profile: :draft_18}
  end

  def publish_media(%__MODULE__{task: task}), do: send(task.pid, :publish_media)
  def finish_media(%__MODULE__{task: task}), do: send(task.pid, :finish_media)

  def await_datagrams(%__MODULE__{task: task}) do
    pid = task.pid

    receive do
      {:datagrams_sent, ^pid} -> :ok
    after
      @timeout -> {:error, :datagrams_timeout}
    end
  end

  def await_shutdown(%__MODULE__{task: task}) do
    case Task.yield(task, @timeout) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      nil -> {:error, :publisher_shutdown_timeout}
    end
  end

  defp publish(parent, network, config) do
    with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_18),
         {:ok, listener, ctx} <- Transport.listen(ctx, 0),
         {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
      send(parent, {:publisher_ready, port})
      serve(ctx, listener, port, config, parent)
    end
  end

  defp serve(ctx, listener, port, config, parent) do
    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, control, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_client_setup(ctx, control, port),
         {:ok, ctx} <- send_server_setup(ctx, conn),
         {:ok, catalog_request, catalog_subscribe, ctx} <-
           receive_subscription(ctx, conn, config.namespace, "catalog"),
         {:ok, ctx} <- accept_subscription(ctx, catalog_request, catalog_subscribe.request_id),
         {:ok, ctx} <-
           publish_subgroup(ctx, conn, catalog_subscribe.request_id, config.catalog, false),
         {:ok, ctx} <- finish_subscription(ctx, catalog_request, 1),
         {:ok, media_request, media_subscribe, ctx} <-
           receive_subscription(ctx, conn, config.namespace, config.track_name),
         {:ok, ctx} <- accept_subscription(ctx, media_request, media_subscribe.request_id),
         :ok <- await_publish(),
         {:ok, ctx} <-
           publish_objects(
             ctx,
             conn,
             media_subscribe.request_id,
             config.payload,
             config.opts,
             parent
           ),
         {:ok, ctx} <- finish_subscription(ctx, media_request, 1),
         {:ok, _event, _ctx} <- receive_connection_close(ctx, conn) do
      :ok
    end
  end

  defp publish_objects(ctx, conn, track_alias, objects, [delivery: :datagram], parent) do
    result =
      Enum.reduce_while(objects, {:ok, ctx}, fn object, {:ok, ctx} ->
        case Transport.send_datagram(ctx, conn, Codec.encode_datagram(track_alias, object)) do
          {:ok, ctx} -> {:cont, {:ok, ctx}}
          error -> {:halt, error}
        end
      end)

    with {:ok, ctx} <- result do
      send(parent, {:datagrams_sent, self()})

      receive do
        :finish_media -> {:ok, ctx}
      after
        @close_timeout -> {:error, :finish_timeout}
      end
    end
  end

  defp publish_objects(ctx, conn, track_alias, payload, _opts, _parent) do
    publish_subgroup(ctx, conn, track_alias, payload, true)
  end

  defp await_publish do
    receive do
      :publish_media -> :ok
    after
      @close_timeout -> {:error, :publish_timeout}
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

  defp receive_client_setup(ctx, control, port) do
    setup = Codec.client_setup(URI.parse("moqt://localhost:#{port}"))

    case Transport.recv_stream(ctx, control, byte_size(setup)) do
      {:ok, ^setup, ctx} -> {:ok, ctx}
      other -> {:error, {:unexpected_client_setup, other}}
    end
  end

  defp send_server_setup(ctx, conn) do
    with {:ok, control, ctx} <- Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, control, <<0xAF, 0, 0, 0>>) do
      {:ok, ctx}
    end
  end

  defp receive_subscription(ctx, conn, namespace, track_name) do
    with {:ok, request, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, 0x03, payload, ctx} <- receive_control_frame(ctx, request),
         {:ok, subscribe} <- Codec.decode_subscribe(payload),
         true <-
           subscribe.track_namespace == namespace and subscribe.track_name == track_name do
      {:ok, request, subscribe, ctx}
    else
      other -> {:error, {:unexpected_subscribe, other}}
    end
  end

  defp accept_subscription(ctx, request, track_alias) do
    case Transport.send_stream(
           ctx,
           request,
           Codec.subscribe_ok(track_alias, expires: 0, group_order: :ascending)
         ) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:subscribe_accept_failed, other}}
    end
  end

  defp publish_subgroup(ctx, conn, track_alias, payload, include_continuation?) do
    first = %MOQX.Object{
      group_id: if(include_continuation?, do: 1, else: 0),
      subgroup_id: 0,
      object_id: 0,
      payload: payload,
      end_of_group?: not include_continuation?
    }

    bytes = Codec.encode_subgroup(track_alias, first)

    bytes =
      if include_continuation? do
        second = %{first | object_id: 1, payload: "later", end_of_group?: false}
        {:ok, continuation} = Codec.encode_subgroup_object(first.object_id, second)
        [bytes, continuation]
      else
        bytes
      end

    with {:ok, stream, ctx} <- Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, stream, bytes, finish: true) do
      {:ok, ctx}
    end
  end

  defp finish_subscription(ctx, request, stream_count) do
    case Transport.send_stream(
           ctx,
           request,
           Codec.publish_done(2, stream_count, "track ended"),
           finish: true
         ) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:publish_done_failed, other}}
    end
  end

  defp receive_control_frame(ctx, stream) do
    with {:ok, <<type, length::16>>, ctx} <- Transport.recv_stream(ctx, stream, 3),
         {:ok, payload, ctx} <- Transport.recv_stream(ctx, stream, length) do
      {:ok, type, payload, ctx}
    end
  end
end
