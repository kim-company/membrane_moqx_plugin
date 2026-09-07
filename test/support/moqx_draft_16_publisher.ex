defmodule Membrane.MOQX.TestDraft16Publisher do
  @moduledoc false

  import Bitwise

  alias MOQX.Protocol.MOQTDraft16.Codec
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
      @timeout -> raise "draft-16 test publisher did not start"
    end
  end

  def transport(%__MODULE__{network: network}) do
    {Support, network: network, profile: :draft_16}
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
    with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_16),
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
         {:ok, ctx} <- receive_setup(ctx, control, port),
         {:ok, ctx} <- accept_subscription(ctx, control, config.namespace, "catalog", 0, 7),
         {:ok, catalog_stream, ctx} <-
           Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, catalog_stream, subgroup(7, 0, config.catalog)),
         {:ok, ctx} <-
           accept_subscription(ctx, control, config.namespace, config.track_name, 2, 8),
         :ok <- await_publish(),
         {:ok, ctx} <- publish_objects(ctx, conn, config.payload, config.opts, parent),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, control, Codec.publish_done(2, 2, 1, "track ended")),
         {:ok, _event, _ctx} <- receive_connection_close(ctx, conn) do
      :ok
    end
  end

  defp publish_objects(ctx, conn, objects, [delivery: :datagram], parent) do
    result =
      Enum.reduce_while(objects, {:ok, ctx}, fn object, {:ok, ctx} ->
        case Transport.send_datagram(ctx, conn, Codec.encode_datagram(8, object)) do
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

  defp publish_objects(ctx, conn, payload, _opts, _parent) do
    with {:ok, media_stream, ctx} <-
           Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             media_stream,
             [subgroup(8, 1, payload), encode_varint(0), encode_bytes("later")],
             finish: true
           ) do
      {:ok, ctx}
    end
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

  defp receive_setup(ctx, control, port) do
    setup =
      frame(0x20, [
        encode_varint(3),
        encode_bytes_parameter(1, ""),
        encode_integer_parameter(1, 100),
        encode_bytes_parameter(3, "localhost:#{port}")
      ])

    with {:ok, ^setup, ctx} <- Transport.recv_stream(ctx, control, byte_size(setup)),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, control, <<0x21, 0, 1, 0>>) do
      {:ok, ctx}
    end
  end

  defp accept_subscription(ctx, control, namespace, track, request_id, alias_id) do
    expected = subscribe_frame(request_id, namespace, track)

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             control,
             <<0x04, 0, 3, request_id, alias_id, 0>>
           ),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, control, <<0x15, 0, 1, request_id + 2>>) do
      {:ok, ctx}
    end
  end

  defp subgroup(track_alias, group_id, payload) do
    IO.iodata_to_binary([
      <<0x34, track_alias, group_id, 0, 0>>,
      encode_varint(byte_size(payload)),
      payload
    ])
  end

  defp subscribe_frame(request_id, namespace, track) do
    frame(0x03, [
      encode_varint(request_id),
      encode_tuple(namespace),
      encode_bytes(track),
      encode_varint(2),
      encode_integer_parameter(0x20, 128),
      encode_bytes_parameter(1, encode_varint(2))
    ])
  end

  defp frame(type, payload) do
    payload = IO.iodata_to_binary(payload)
    IO.iodata_to_binary([encode_varint(type), <<byte_size(payload)::16>>, payload])
  end

  defp encode_tuple(fields),
    do: [encode_varint(length(fields)) | Enum.map(fields, &encode_bytes/1)]

  defp encode_bytes(value), do: [encode_varint(byte_size(value)), value]
  defp encode_integer_parameter(delta, value), do: [encode_varint(delta), encode_varint(value)]
  defp encode_bytes_parameter(delta, value), do: [encode_varint(delta), encode_bytes(value)]
  defp encode_varint(value) when value < 64, do: <<value>>
  defp encode_varint(value) when value < 16_384, do: <<value ||| 0x4000::16>>
end
