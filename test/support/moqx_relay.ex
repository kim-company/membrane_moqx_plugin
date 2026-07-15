defmodule Membrane.MOQX.TestRelay do
  @moduledoc false

  alias MOQX.Protocol.MOQTDraft14.{Codec, Messages}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 1_000

  defstruct [:task, :network, :endpoint]

  def start(namespace, options \\ []) do
    {:ok, network} = Support.start_network()
    parent = self()

    task = Task.async(fn -> relay(parent, network, namespace, options) end)

    receive do
      {:relay_ready, port} ->
        %__MODULE__{
          task: task,
          network: network,
          endpoint: "moqt://localhost:#{port}"
        }
    after
      @timeout -> raise "test relay did not start"
    end
  end

  def transport(%__MODULE__{network: network}) do
    {Support, network: network, profile: :draft_14}
  end

  def capture(%__MODULE__{task: task}, tracks) do
    send(task.pid, {:capture, self(), tracks})

    receive do
      {:captured, objects} -> {:ok, objects}
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :capture_timeout}
    end
  end

  def await_shutdown(%__MODULE__{task: task}) do
    send(task.pid, {:await_shutdown, self()})

    receive do
      :relay_shutdown -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :shutdown_timeout}
    end
  end

  def capture_many(%__MODULE__{task: task}, track, count) do
    send(task.pid, {:capture_many, self(), track, count})

    receive do
      {:captured_many, objects} -> {:ok, objects}
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :capture_timeout}
    end
  end

  def cancel_publication(%__MODULE__{task: task}, error_code, reason) do
    send(task.pid, {:cancel_publication, self(), error_code, reason})

    receive do
      :publication_cancelled -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :cancel_timeout}
    end
  end

  def close_connection(%__MODULE__{task: task}, error_code) do
    send(task.pid, {:close_connection, self(), error_code})

    receive do
      :connection_closed -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :close_timeout}
    end
  end

  def fail_protocol(%__MODULE__{task: task}) do
    send(task.pid, {:fail_protocol, self()})

    receive do
      :protocol_frame_sent -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :protocol_failure_timeout}
    end
  end

  defp relay(parent, network, namespace, options) do
    with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_14),
         {:ok, listener, ctx} <- Transport.listen(ctx, 0),
         {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
      send(parent, {:relay_ready, port})
      accept_publisher(ctx, listener, namespace, Keyword.get(options, :publication, :accept))
    end
  end

  defp accept_publisher(ctx, listener, namespace, publication_response) do
    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, control, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_client_setup(ctx, control),
         {:ok, ctx} <- send_server_setup(ctx, control),
         {:ok, ctx} <- receive_publication(ctx, control, namespace),
         {:ok, ctx} <- respond_to_publication(ctx, control, publication_response) do
      case publication_response do
        :accept -> relay_loop(ctx, conn, control, namespace, 1)
        {:reject, _error_code, _reason} -> rejected_loop(ctx, conn)
      end
    end
  end

  defp receive_client_setup(ctx, control) do
    setup = Codec.client_setup()

    case Transport.recv_stream(ctx, control, byte_size(setup)) do
      {:ok, ^setup, ctx} -> {:ok, ctx}
      other -> {:error, {:unexpected_client_setup, other}}
    end
  end

  defp send_server_setup(ctx, control) do
    server_setup = <<0x21, 0, 9, 0xC0000000FF00000E::64, 0>>

    case Transport.send_stream(ctx, control, server_setup) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:server_setup_failed, other}}
    end
  end

  defp receive_publication(ctx, control, namespace) do
    message =
      Codec.encode(%Messages.PublishNamespace{
        request_id: 0,
        track_namespace: namespace
      })

    case Transport.recv_stream(ctx, control, byte_size(message)) do
      {:ok, ^message, ctx} -> {:ok, ctx}
      other -> {:error, {:unexpected_publication, other}}
    end
  end

  defp respond_to_publication(ctx, control, :accept) do
    message = <<0x07, 0, 1, 0>>

    case Transport.send_stream(ctx, control, message) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:publication_accept_failed, other}}
    end
  end

  defp respond_to_publication(ctx, control, {:reject, error_code, reason})
       when error_code in 0..63 and byte_size(reason) < 64 do
    message = <<0x08, 0, byte_size(reason) + 3, 0, error_code, byte_size(reason), reason::binary>>

    case Transport.send_stream(ctx, control, message) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:publication_reject_failed, other}}
    end
  end

  defp rejected_loop(ctx, conn) do
    receive do
      {:await_shutdown, caller} ->
        case receive_connection_close(ctx, conn) do
          {:ok, _ctx} -> send(caller, :relay_shutdown)
          {:error, reason, _ctx} -> send(caller, {:relay_error, reason})
        end
    after
      @timeout * 10 -> :ok
    end
  end

  defp relay_loop(ctx, conn, control, namespace, next_request_id) do
    receive do
      {:capture, _caller, _tracks} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:await_shutdown, _caller} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:capture_many, _caller, _track, _count} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:cancel_publication, _caller, _error_code, _reason} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:close_connection, _caller, _error_code} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:fail_protocol, _caller} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)
    after
      @timeout * 10 -> :ok
    end
  end

  defp handle_relay_message({:capture, caller, tracks}, ctx, conn, control, namespace, next_id) do
    {results, ctx} =
      Enum.map_reduce(Enum.with_index(tracks), ctx, fn {track, index}, ctx ->
        capture_track(ctx, conn, control, namespace, track, next_id + index * 2)
      end)

    send_capture_result(caller, results)
    relay_loop(ctx, conn, control, namespace, next_id + length(tracks) * 2)
  end

  defp handle_relay_message({:await_shutdown, caller}, ctx, conn, control, namespace, _next_id) do
    case receive_shutdown(ctx, conn, control, namespace) do
      {:ok, _ctx} -> send(caller, :relay_shutdown)
      {:error, reason, _ctx} -> send(caller, {:relay_error, reason})
    end
  end

  defp handle_relay_message(
         {:capture_many, caller, track, count},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      case capture_track_many(ctx, conn, control, namespace, track, count, next_id) do
        {:ok, objects, ctx} ->
          send(caller, {:captured_many, objects})
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id + 2)
  end

  defp handle_relay_message(
         {:cancel_publication, caller, error_code, reason},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    message = encode_publication_cancel(namespace, error_code, reason)

    ctx =
      case Transport.send_stream(ctx, control, message) do
        {:ok, _send, ctx} ->
          send(caller, :publication_cancelled)
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message({:close_connection, caller, error_code}, ctx, conn, _control, _, _) do
    case Transport.close_connection(ctx, conn, error_code) do
      {:ok, _ctx} -> send(caller, :connection_closed)
      {:error, reason, _ctx} -> send(caller, {:relay_error, reason})
    end
  end

  defp handle_relay_message({:fail_protocol, caller}, ctx, _conn, control, _namespace, _next_id) do
    invalid_publish_namespace_ok = <<0x07, 0, 0>>

    case Transport.send_stream(ctx, control, invalid_publish_namespace_ok) do
      {:ok, _send, _ctx} -> send(caller, :protocol_frame_sent)
      {:error, reason, _ctx} -> send(caller, {:relay_error, reason})
    end
  end

  defp send_capture_result(caller, results) do
    case Enum.find(results, &match?({:error, _reason}, &1)) do
      nil -> send(caller, {:captured, Map.new(results, fn {:ok, item} -> item end)})
      {:error, reason} -> send(caller, {:relay_error, reason})
    end
  end

  defp capture_track(ctx, conn, control, namespace, track, request_id) do
    subscribe =
      Codec.encode(%Messages.Subscribe{
        request_id: request_id,
        track_namespace: namespace,
        track_name: track,
        filter_type: :largest_object
      })

    with {:ok, _send, ctx} <- Transport.send_stream(ctx, control, subscribe),
         {:ok, ctx} <- receive_subscribe_ok(ctx, control, request_id),
         {:ok, stream, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- Transport.set_active(ctx, stream, true),
         {:ok, object, ctx} <- receive_object(ctx, stream, <<>>),
         {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             control,
             Codec.encode(%Messages.Unsubscribe{request_id: request_id})
           ),
         {:ok, ctx} <- receive_publish_done(ctx, control, request_id) do
      {{:ok, {track, object}}, ctx}
    else
      {:error, reason, ctx} -> {{:error, reason}, ctx}
      {:error, reason} -> {{:error, reason}, ctx}
    end
  end

  defp capture_track_many(ctx, conn, control, namespace, track, count, request_id) do
    subscribe =
      Codec.encode(%Messages.Subscribe{
        request_id: request_id,
        track_namespace: namespace,
        track_name: track,
        filter_type: :largest_object
      })

    with {:ok, _send, ctx} <- Transport.send_stream(ctx, control, subscribe),
         {:ok, ctx} <- receive_subscribe_ok(ctx, control, request_id),
         {:ok, objects, ctx} <- receive_objects(ctx, conn, count),
         {:ok, _send, ctx} <-
           Transport.send_stream(
             ctx,
             control,
             Codec.encode(%Messages.Unsubscribe{request_id: request_id})
           ),
         {:ok, ctx} <- receive_publish_done(ctx, control, request_id) do
      {:ok, objects, ctx}
    end
  end

  defp receive_objects(ctx, conn, count) do
    Enum.reduce_while(1..count, {:ok, [], ctx}, fn _index, {:ok, objects, ctx} ->
      with {:ok, stream, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
           {:ok, ctx} <- Transport.set_active(ctx, stream, true),
           {:ok, object, ctx} <- receive_object(ctx, stream, <<>>) do
        {:cont, {:ok, [object | objects], ctx}}
      else
        {:error, reason, ctx} -> {:halt, {:error, reason, ctx}}
        {:error, reason} -> {:halt, {:error, reason, ctx}}
      end
    end)
    |> case do
      {:ok, objects, ctx} -> {:ok, Enum.reverse(objects), ctx}
      error -> error
    end
  end

  defp receive_subscribe_ok(ctx, control, request_id) do
    with {:ok, type, payload, ctx} <- receive_control_frame(ctx, control),
         {:ok, %Messages.SubscribeOk{request_id: ^request_id}} <-
           decode_subscribe_response(type, payload) do
      {:ok, ctx}
    else
      other -> {:error, {:unexpected_subscribe_response, other}}
    end
  end

  defp decode_subscribe_response(0x04, payload), do: Codec.decode_subscribe_ok(payload)
  defp decode_subscribe_response(0x05, payload), do: Codec.decode_subscribe_error(payload)
  defp decode_subscribe_response(type, payload), do: {:error, {:unexpected_type, type, payload}}

  defp receive_publish_done(ctx, control, request_id) do
    with {:ok, 0x0B, payload, ctx} <- receive_control_frame(ctx, control),
         {:ok, %Messages.PublishDone{request_id: ^request_id}} <-
           Codec.decode_publish_done(payload) do
      {:ok, ctx}
    else
      other -> {:error, {:unexpected_publish_done, other}}
    end
  end

  defp receive_shutdown(ctx, conn, control, namespace) do
    expected = Codec.encode(%Messages.PublishNamespaceDone{track_namespace: namespace})

    with {:ok, ^expected, ctx} <- Transport.recv_stream(ctx, control, byte_size(expected)),
         {:ok, ctx} <- receive_connection_close(ctx, conn) do
      {:ok, ctx}
    else
      {:error, reason, ctx} -> {:error, reason, ctx}
      other -> {:error, {:unexpected_shutdown, other}, ctx}
    end
  end

  defp receive_connection_close(ctx, conn) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:connection_event, ^conn, :closed, _metadata}, ctx} -> {:ok, ctx}
      {:ok, _event, ctx} -> receive_connection_close(ctx, conn)
      {:unknown, _message, ctx} -> receive_connection_close(ctx, conn)
      {:timeout, ctx} -> {:error, :connection_close_timeout, ctx}
    end
  end

  defp receive_control_frame(ctx, control) do
    with {:ok, <<type, length::16>>, ctx} <- Transport.recv_stream(ctx, control, 3),
         {:ok, payload, ctx} <- Transport.recv_stream(ctx, control, length) do
      {:ok, type, payload, ctx}
    end
  end

  defp encode_publication_cancel(namespace, error_code, reason)
       when error_code in 0..63 and byte_size(reason) < 64 do
    namespace =
      [length(namespace) | Enum.flat_map(namespace, &[byte_size(&1), &1])]
      |> IO.iodata_to_binary()

    payload = namespace <> <<error_code, byte_size(reason)>> <> reason
    <<0x0C, byte_size(payload)::16, payload::binary>>
  end

  defp receive_object(ctx, stream, bytes) do
    case Codec.decode_subgroup_object(bytes) do
      {:ok, object, <<>>} ->
        {:ok, object, ctx}

      :more ->
        receive_object_event(ctx, stream, bytes)

      other ->
        {:error, {:invalid_subgroup, other}, ctx}
    end
  end

  defp receive_object_event(ctx, stream, bytes) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:stream_data, ^stream, data, _metadata}, ctx} ->
        receive_object(ctx, stream, bytes <> data)

      {:ok, {:stream_event, ^stream, :peer_finished_sending, _metadata}, ctx} ->
        receive_object_event(ctx, stream, bytes)

      {:ok, _event, ctx} ->
        receive_object_event(ctx, stream, bytes)

      {:timeout, ctx} ->
        {:error, :object_timeout, ctx}

      {:unknown, _message, ctx} ->
        receive_object_event(ctx, stream, bytes)
    end
  end
end
