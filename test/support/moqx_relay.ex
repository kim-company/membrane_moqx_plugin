defmodule Membrane.MOQX.TestRelay do
  @moduledoc false

  alias MOQX.Protocol.MOQTDraft18.{Codec, SubgroupDecoder}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 5_000

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
    {Support, network: network, profile: :draft_18}
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

  def await_connection_close(%__MODULE__{task: task}) do
    send(task.pid, {:await_connection_close, self()})

    receive do
      :connection_closed -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :connection_close_timeout}
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

  def subscribe(%__MODULE__{task: task}, track, options \\ []) do
    send(task.pid, {:subscribe, self(), track, options})

    receive do
      {:subscription_result, result} -> result
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :subscribe_timeout}
    end
  end

  def request_subscription(%__MODULE__{task: task}, track, options \\ []) do
    send(task.pid, {:request_subscription, self(), track, options})

    receive do
      {:subscription_sent, request_id} -> {:ok, request_id}
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :subscription_request_timeout}
    end
  end

  def await_subscription_result(%__MODULE__{task: task}, request_id) do
    send(task.pid, {:await_subscription_result, self(), request_id})

    receive do
      {:subscription_result, result} -> result
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :subscribe_timeout}
    end
  end

  def await_subscription_results(%__MODULE__{task: task}, request_ids) do
    send(task.pid, {:await_subscription_results, self(), request_ids})

    receive do
      {:subscription_results, results} -> {:ok, results}
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :subscribe_timeout}
    end
  end

  def cancel_pending_subscription(%__MODULE__{task: task}, request_id) do
    send(task.pid, {:cancel_pending_subscription, self(), request_id})

    receive do
      :pending_subscription_cancelled -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :pending_unsubscribe_timeout}
    end
  end

  def receive_subscription_objects(%__MODULE__{task: task}, count) do
    send(task.pid, {:receive_subscription_objects, self(), count})

    receive do
      {:subscription_objects, objects} -> {:ok, objects}
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :subscription_objects_timeout}
    end
  end

  def unsubscribe(%__MODULE__{task: task}, request_id) do
    send(task.pid, {:unsubscribe, self(), request_id})

    receive do
      :unsubscribed -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :unsubscribe_timeout}
    end
  end

  def await_publisher_finish(%__MODULE__{task: task}, request_id) do
    send(task.pid, {:await_publisher_finish, self(), request_id})

    receive do
      :publisher_finished_subscription -> :ok
      {:relay_error, reason} -> {:error, reason}
    after
      @timeout -> {:error, :publisher_finish_timeout}
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

  defp relay(parent, network, namespace, options) do
    with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_18),
         {:ok, listener, ctx} <- Transport.listen(ctx, 0),
         {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
      send(parent, {:relay_ready, port})

      accept_publisher(
        ctx,
        listener,
        port,
        namespace,
        Keyword.get(options, :publication, :accept)
      )
    end
  end

  defp accept_publisher(ctx, listener, port, namespace, publication_response) do
    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, control, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_client_setup(ctx, control, port),
         {:ok, _server_control, ctx} <- send_server_setup(ctx, conn),
         {:ok, publication, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_publication(ctx, publication, namespace),
         {:ok, ctx} <- respond_to_publication(ctx, publication, publication_response) do
      Process.put(:publication_stream, publication)
      Process.put(:request_streams, %{})
      Process.put(:pending_data_streams, [])
      Process.put(:active_data_streams, %{})
      Process.put(:pending_objects, %{})
      Process.put(:published_track_aliases, %{})
      Process.put(:subscription_tracks, %{})
      Process.put(:subscription_aliases, MapSet.new())

      case publication_response do
        :accept -> relay_loop(ctx, conn, control, namespace, 1)
        {:reject, _error_code, _reason} -> rejected_loop(ctx, conn)
      end
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
      {:ok, control, ctx}
    end
  end

  defp receive_publication(ctx, control, namespace) do
    message = Codec.publish_namespace(0, namespace)

    case Transport.recv_stream(ctx, control, byte_size(message)) do
      {:ok, ^message, ctx} -> {:ok, ctx}
      other -> {:error, {:unexpected_publication, other}}
    end
  end

  defp respond_to_publication(ctx, control, :accept) do
    message = Codec.request_ok()

    case Transport.send_stream(ctx, control, message) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:publication_accept_failed, other}}
    end
  end

  defp respond_to_publication(ctx, control, {:reject, error_code, reason})
       when error_code in 0..63 and byte_size(reason) < 64 do
    message = Codec.request_error(error_code, reason)

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

      {:await_connection_close, _caller} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:capture_many, _caller, _track, _count} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:subscribe, _caller, _track, _options} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:request_subscription, _caller, _track, _options} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:await_subscription_result, _caller, _request_id} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:await_subscription_results, _caller, _request_ids} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:cancel_pending_subscription, _caller, _request_id} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:receive_subscription_objects, _caller, _count} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:unsubscribe, _caller, _request_id} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:await_publisher_finish, _caller, _request_id} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:cancel_publication, _caller, _error_code, _reason} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)

      {:close_connection, _caller, _error_code} = message ->
        handle_relay_message(message, ctx, conn, control, namespace, next_request_id)
    after
      10 ->
        case accept_publisher_request(ctx, conn) do
          {:ok, ctx} -> relay_loop(ctx, conn, control, namespace, next_request_id)
          {:none, ctx} -> relay_loop(ctx, conn, control, namespace, next_request_id)
          {:error, _reason, _ctx} -> :ok
        end
    end
  end

  # PUBLISH_TRACK requests are independently scoped bidirectional streams.  Keep
  # accepting them while the helper is otherwise idle so AddTrack can complete
  # before a test asks this relay to subscribe or capture.
  defp accept_publisher_request(ctx, conn) do
    case Transport.accept_stream(ctx, conn, [], 10) do
      {:ok, %{info: %{direction: :bidirectional}} = stream, ctx} ->
        with {:ok, type, payload, ctx} <- receive_control_frame(ctx, stream),
             true <- type == 0x1D,
             {:ok, request_id, track_name, track_alias} <- decode_publish_track(payload),
             {:ok, _send, ctx} <- Transport.send_stream(ctx, stream, Codec.request_ok()) do
          Process.put(:request_streams, Map.put(request_streams(), request_id, stream))

          Process.put(
            :published_track_aliases,
            Map.put(Process.get(:published_track_aliases, %{}), track_name, track_alias)
          )

          {:ok, ctx}
        else
          other -> {:error, {:unexpected_publisher_request, other}, ctx}
        end

      {:ok, stream, ctx} ->
        Process.put(:pending_data_streams, pending_data_streams() ++ [stream])
        {:ok, ctx}

      {:error, :timeout, ctx} ->
        {:none, ctx}

      {:error, reason, ctx} ->
        {:error, reason, ctx}
    end
  end

  defp request_streams, do: Process.get(:request_streams, %{})
  defp pending_data_streams, do: Process.get(:pending_data_streams, [])

  defp accept_data_stream(ctx, conn) do
    case pending_data_streams() do
      [stream | rest] ->
        Process.put(:pending_data_streams, rest)
        {:ok, stream, ctx}

      [] ->
        Transport.accept_stream(ctx, conn, [], @timeout)
    end
  end

  defp open_subscription(ctx, conn, request_id, namespace, track, options) do
    bytes =
      Codec.subscribe(request_id, %MOQX.TrackRef{namespace: namespace, track: track}, options)

    with {:ok, stream, ctx} <- Transport.open_stream(ctx, conn, direction: :bidirectional),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, stream, bytes) do
      Process.put(:request_streams, Map.put(request_streams(), request_id, stream))

      Process.put(
        :subscription_tracks,
        Map.put(Process.get(:subscription_tracks, %{}), request_id, track)
      )

      {:ok, stream, ctx}
    end
  end

  defp subscription_stream(request_id), do: Map.fetch(request_streams(), request_id)

  defp abort_subscription(ctx, request_id) do
    with {:ok, stream} <- subscription_stream(request_id),
         {:ok, ctx} <- Transport.abort_sending(ctx, stream, 0x01),
         {:ok, ctx} <- Transport.abort_receiving(ctx, stream, 0x01) do
      {:ok, ctx}
    else
      :error -> {:error, {:unknown_subscription, request_id}, ctx}
      error -> error
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
         {:await_connection_close, caller},
         ctx,
         conn,
         _control,
         _namespace,
         _next_id
       ) do
    case receive_connection_close(ctx, conn) do
      {:ok, _ctx} -> send(caller, :connection_closed)
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
         {:subscribe, caller, track, options},
         ctx,
         conn,
         control,
         namespace,
         request_id
       ) do
    ctx =
      with {:ok, stream, ctx} <-
             open_subscription(ctx, conn, request_id, namespace, track, options),
           {:ok, result, ctx} <- receive_subscription_result(ctx, stream, request_id) do
        send(caller, {:subscription_result, result})
        ctx
      else
        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, request_id + 2)
  end

  defp handle_relay_message(
         {:request_subscription, caller, track, options},
         ctx,
         conn,
         control,
         namespace,
         request_id
       ) do
    ctx =
      case open_subscription(ctx, conn, request_id, namespace, track, options) do
        {:ok, _stream, ctx} ->
          send(caller, {:subscription_sent, request_id})
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, request_id + 2)
  end

  defp handle_relay_message(
         {:await_subscription_result, caller, request_id},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      case subscription_stream(request_id) do
        {:ok, stream} -> receive_subscription_result(ctx, stream, request_id)
        :error -> {:error, {:unknown_subscription, request_id}, ctx}
      end
      |> case do
        {:ok, result, ctx} ->
          send(caller, {:subscription_result, result})
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message(
         {:await_subscription_results, caller, request_ids},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    result =
      Enum.reduce_while(request_ids, {:ok, %{}, ctx}, fn request_id, {:ok, results, ctx} ->
        case subscription_stream(request_id) do
          {:ok, stream} ->
            receive_and_accumulate_subscription(ctx, stream, request_id, results)

          :error ->
            {:halt, {:error, {:unknown_subscription, request_id}, ctx}}
        end
      end)

    ctx =
      case result do
        {:ok, results, ctx} ->
          send(caller, {:subscription_results, results})
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message(
         {:cancel_pending_subscription, caller, request_id},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      case abort_subscription(ctx, request_id) do
        {:ok, ctx} ->
          send(caller, :pending_subscription_cancelled)
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message(
         {:receive_subscription_objects, caller, count},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      case receive_objects(ctx, conn, count) do
        {:ok, objects, ctx} ->
          send(caller, {:subscription_objects, objects})
          ctx

        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message(
         {:unsubscribe, caller, request_id},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      with {:ok, stream} <- subscription_stream(request_id),
           {:ok, ctx} <- Transport.abort_sending(ctx, stream, 0x01),
           {:ok, ctx} <- Transport.abort_receiving(ctx, stream, 0x01) do
        send(caller, :unsubscribed)
        ctx
      else
        {:error, reason, ctx} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message(
         {:await_publisher_finish, caller, request_id},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      case subscription_stream(request_id) do
        {:ok, stream} -> receive_publish_done(ctx, stream, request_id)
        :error -> {:error, {:unknown_subscription, request_id}}
      end
      |> case do
        {:ok, ctx} ->
          send(caller, :publisher_finished_subscription)
          ctx

        {:error, reason} ->
          send(caller, {:relay_error, reason})
          ctx
      end

    relay_loop(ctx, conn, control, namespace, next_id)
  end

  defp handle_relay_message(
         {:cancel_publication, caller, error_code, _reason},
         ctx,
         conn,
         control,
         namespace,
         next_id
       ) do
    ctx =
      with stream when not is_nil(stream) <- Process.get(:publication_stream),
           {:ok, ctx} <- Transport.abort_sending(ctx, stream, error_code),
           {:ok, ctx} <- Transport.abort_receiving(ctx, stream, error_code) do
        send(caller, :publication_cancelled)
        ctx
      else
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

  defp receive_and_accumulate_subscription(ctx, stream, request_id, results) do
    case receive_subscription_result(ctx, stream, request_id) do
      {:ok, response, ctx} ->
        {:cont, {:ok, Map.put(results, request_id, response), ctx}}

      {:error, reason, ctx} ->
        {:halt, {:error, reason, ctx}}
    end
  end

  defp send_capture_result(caller, results) do
    case Enum.find(results, &match?({:error, _reason}, &1)) do
      nil -> send(caller, {:captured, Map.new(results, fn {:ok, item} -> item end)})
      {:error, reason} -> send(caller, {:relay_error, reason})
    end
  end

  defp capture_track(ctx, conn, _control, namespace, track, request_id) do
    with {:ok, request, ctx} <-
           open_subscription(ctx, conn, request_id, namespace, track,
             filter: %MOQX.SubscriptionFilter{type: :largest_object}
           ),
         {:ok, _subscription_alias, ctx} <- receive_subscribe_ok(ctx, request, request_id),
         {:ok, track_alias} <- published_track_alias(track),
         {:ok, object, ctx} <- receive_latest_object_for_alias(ctx, conn, track_alias),
         {:ok, ctx} <- Transport.abort_sending(ctx, request, 0x01),
         {:ok, ctx} <- Transport.abort_receiving(ctx, request, 0x01) do
      {{:ok, {track, object}}, ctx}
    else
      {:error, reason, ctx} -> {{:error, reason}, ctx}
      {:error, reason} -> {{:error, reason}, ctx}
    end
  end

  defp capture_track_many(ctx, conn, _control, namespace, track, count, request_id) do
    with {:ok, request, ctx} <-
           open_subscription(ctx, conn, request_id, namespace, track,
             filter: %MOQX.SubscriptionFilter{type: :largest_object}
           ),
         {:ok, _subscription_alias, ctx} <- receive_subscribe_ok(ctx, request, request_id),
         {:ok, track_alias} <- published_track_alias(track),
         {:ok, objects, ctx} <- receive_objects(ctx, conn, count, MapSet.new([track_alias])),
         {:ok, ctx} <- Transport.abort_sending(ctx, request, 0x01),
         {:ok, ctx} <- Transport.abort_receiving(ctx, request, 0x01) do
      {:ok, objects, ctx}
    end
  end

  defp receive_objects(ctx, conn, count) do
    receive_objects(ctx, conn, count, Process.get(:subscription_aliases, MapSet.new()))
  end

  defp receive_objects(ctx, conn, count, aliases) do
    Enum.reduce_while(1..count, {:ok, [], ctx}, fn _index, {:ok, objects, ctx} ->
      case receive_object_for_aliases(ctx, conn, aliases) do
        {:ok, object, ctx} -> {:cont, {:ok, [object | objects], ctx}}
        {:error, reason, ctx} -> {:halt, {:error, reason, ctx}}
        {:error, reason} -> {:halt, {:error, reason, ctx}}
      end
    end)
    |> case do
      {:ok, objects, ctx} -> {:ok, Enum.reverse(objects), ctx}
      error -> error
    end
  end

  defp receive_subscribe_ok(ctx, control, _request_id) do
    with {:ok, type, payload, ctx} <- receive_control_frame(ctx, control),
         {:ok, %{track_alias: track_alias}} <- decode_subscribe_response(type, payload) do
      {:ok, track_alias, ctx}
    else
      other -> {:error, {:unexpected_subscribe_response, other}}
    end
  end

  defp receive_subscription_result(ctx, control, request_id) do
    case receive_subscription_result(ctx, control) do
      {:ok, track_alias, {:ok, track_alias}, ctx} ->
        track = Process.get(:subscription_tracks, %{})[request_id]
        track_alias = Map.get(Process.get(:published_track_aliases, %{}), track, track_alias)

        Process.put(
          :subscription_aliases,
          MapSet.put(Process.get(:subscription_aliases, MapSet.new()), track_alias)
        )

        {:ok, {:ok, request_id}, ctx}

      {:ok, _track_alias, {:error, _error} = result, ctx} ->
        {:ok, result, ctx}

      other ->
        {:error, {:unexpected_subscribe_response, other}, ctx}
    end
  end

  defp receive_subscription_result(ctx, control) do
    with {:ok, type, payload, ctx} <- receive_control_frame(ctx, control),
         {:ok, response} <- decode_subscribe_response(type, payload) do
      result =
        case response do
          %{track_alias: track_alias} ->
            {:ok, track_alias}

          %{error_code: code, reason: reason} ->
            {:error, %{code: code, reason: reason}}
        end

      {:ok, Map.get(response, :track_alias, 0), result, ctx}
    else
      other -> {:error, {:unexpected_subscribe_response, other}, ctx}
    end
  end

  defp decode_subscribe_response(0x04, payload), do: Codec.decode_subscribe_ok(payload)
  defp decode_subscribe_response(0x05, payload), do: Codec.decode_request_error(payload)
  defp decode_subscribe_response(type, payload), do: {:error, {:unexpected_type, type, payload}}

  defp decode_publish_track(payload) do
    with {:ok, request_id, rest} <- Codec.decode_varint(payload),
         {:ok, field_count, rest} <- Codec.decode_varint(rest),
         {:ok, _namespace, rest} <- decode_length_prefixed_fields(rest, field_count, []),
         {:ok, track_name, rest} <- decode_length_prefixed(rest),
         {:ok, track_alias, _rest} <- Codec.decode_varint(rest) do
      {:ok, request_id, track_name, track_alias}
    end
  end

  defp decode_length_prefixed_fields(rest, 0, fields),
    do: {:ok, Enum.reverse(fields), rest}

  defp decode_length_prefixed_fields(binary, count, fields) do
    with {:ok, field, rest} <- decode_length_prefixed(binary) do
      decode_length_prefixed_fields(rest, count - 1, [field | fields])
    end
  end

  defp decode_length_prefixed(binary) do
    with {:ok, length, rest} <- Codec.decode_varint(binary),
         true <- byte_size(rest) >= length do
      <<value::binary-size(^length), rest::binary>> = rest
      {:ok, value, rest}
    else
      false -> {:error, :truncated_length_prefixed_value}
      other -> other
    end
  end

  defp published_track_alias(track) do
    case Map.fetch(Process.get(:published_track_aliases, %{}), track) do
      {:ok, track_alias} -> {:ok, track_alias}
      :error -> {:error, {:unknown_published_track, track}}
    end
  end

  defp receive_publish_done(ctx, control, _request_id) do
    with {:ok, 0x0B, payload, ctx} <- receive_control_frame(ctx, control),
         {:ok, _done} <- Codec.decode_publish_done(payload) do
      {:ok, ctx}
    else
      other -> {:error, {:unexpected_publish_done, other}}
    end
  end

  defp receive_shutdown(ctx, conn, _control, _namespace) do
    case receive_connection_close(ctx, conn) do
      {:ok, ctx} -> {:ok, ctx}
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

  defp receive_object(ctx, stream, decoder) do
    case SubgroupDecoder.push(decoder, <<>>) do
      {:ok, decoder, [object | rest]} ->
        Enum.each(rest, &put_pending_object/1)
        put_active_data_stream(object.track_alias, stream, decoder)
        {:ok, object, ctx}

      {:ok, decoder, []} ->
        receive_object_event(ctx, stream, decoder)

      other ->
        {:error, {:invalid_subgroup, other}, ctx}
    end
  end

  defp receive_object_for_alias(ctx, conn, track_alias) do
    receive_object_for_aliases(ctx, conn, MapSet.new([track_alias]))
  end

  defp receive_latest_object_for_alias(ctx, conn, track_alias) do
    streams = pending_data_streams()
    Process.put(:pending_data_streams, [])

    {objects, ctx} =
      Enum.map_reduce(streams, ctx, fn stream, ctx ->
        {:ok, ctx} = Transport.set_active(ctx, stream, true)
        {:ok, object, ctx} = receive_object(ctx, stream, %SubgroupDecoder{})
        {object, ctx}
      end)

    {new_objects, ctx} = drain_available_data_streams(ctx, conn, [])
    objects = objects ++ new_objects

    pending = Process.get(:pending_objects, %{})
    matching = Map.get(pending, track_alias, [])
    Process.put(:pending_objects, Map.delete(pending, track_alias))

    {continued, ctx} = drain_active_objects(ctx, track_alias, [])

    Enum.each(objects, fn object ->
      if object.track_alias != track_alias, do: put_pending_object(object)
    end)

    case Enum.filter(objects, &(&1.track_alias == track_alias)) ++ matching ++ continued do
      [] ->
        receive_object_for_alias(ctx, conn, track_alias)

      objects ->
        latest =
          objects
          |> Enum.with_index()
          |> Enum.max_by(fn {object, index} -> {object.group_id, object.object_id, index} end)
          |> elem(0)

        {:ok, latest, ctx}
    end
  end

  defp receive_object_for_aliases(ctx, conn, aliases) do
    case take_pending_object(aliases) do
      {:ok, object} ->
        {:ok, object, ctx}

      :error ->
        with {:ok, stream, decoder, ctx} <- data_stream_for_aliases(ctx, conn, aliases),
             {:ok, object, ctx} <- receive_object(ctx, stream, decoder) do
          select_object_for_aliases(object, aliases, ctx, conn)
        else
          {:error, :subgroup_finished_without_object, ctx} ->
            receive_object_for_aliases(ctx, conn, aliases)

          other ->
            other
        end
    end
  end

  defp select_object_for_aliases(object, aliases, ctx, conn) do
    if MapSet.member?(aliases, object.track_alias) do
      {:ok, object, ctx}
    else
      put_pending_object(object)
      receive_object_for_aliases(ctx, conn, aliases)
    end
  end

  defp data_stream_for_aliases(ctx, conn, aliases) do
    active = Process.get(:active_data_streams, %{})

    case Enum.find(aliases, &Map.has_key?(active, &1)) do
      nil ->
        with {:ok, stream, ctx} <- accept_data_stream(ctx, conn),
             {:ok, ctx} <- Transport.set_active(ctx, stream, true) do
          {:ok, stream, %SubgroupDecoder{}, ctx}
        end

      track_alias ->
        {stream, decoder} = Map.fetch!(active, track_alias)
        {:ok, stream, decoder, ctx}
    end
  end

  defp take_pending_object(aliases) do
    pending = Process.get(:pending_objects, %{})

    case Enum.find(aliases, &(Map.get(pending, &1, []) != [])) do
      nil ->
        :error

      track_alias ->
        [object | rest] = Map.fetch!(pending, track_alias)
        Process.put(:pending_objects, Map.put(pending, track_alias, rest))
        {:ok, object}
    end
  end

  defp put_pending_object(object) do
    pending = Process.get(:pending_objects, %{})
    objects = Map.get(pending, object.track_alias, [])
    Process.put(:pending_objects, Map.put(pending, object.track_alias, objects ++ [object]))
  end

  defp receive_object_event(ctx, stream, decoder) do
    case Transport.receive_event(ctx, @timeout) do
      {:ok, {:stream_data, ^stream, data, _metadata}, ctx} ->
        case SubgroupDecoder.push(decoder, data) do
          {:ok, decoder, [object | rest]} ->
            Enum.each(rest, &put_pending_object/1)
            put_active_data_stream(object.track_alias, stream, decoder)
            {:ok, object, ctx}

          {:ok, decoder, []} ->
            receive_object_event(ctx, stream, decoder)

          {:error, reason} ->
            {:error, {:invalid_subgroup, reason}, ctx}
        end

      {:ok, {:stream_event, ^stream, :peer_finished_sending, _metadata}, ctx} ->
        remove_active_data_stream(stream)
        {:error, :subgroup_finished_without_object, ctx}

      {:ok, _event, ctx} ->
        receive_object_event(ctx, stream, decoder)

      {:timeout, ctx} ->
        {:error, :object_timeout, ctx}

      {:unknown, _message, ctx} ->
        receive_object_event(ctx, stream, decoder)
    end
  end

  defp put_active_data_stream(track_alias, stream, decoder) do
    active = Process.get(:active_data_streams, %{})
    Process.put(:active_data_streams, Map.put(active, track_alias, {stream, decoder}))
  end

  defp remove_active_data_stream(stream) do
    active = Process.get(:active_data_streams, %{})

    Process.put(
      :active_data_streams,
      Map.reject(active, fn {_alias, {active_stream, _decoder}} -> active_stream == stream end)
    )
  end

  defp drain_active_objects(ctx, track_alias, objects) do
    case Process.get(:active_data_streams, %{})[track_alias] do
      nil ->
        {Enum.reverse(objects), ctx}

      {stream, decoder} ->
        case Transport.receive_event(ctx, 10) do
          {:ok, {:stream_data, ^stream, data, _metadata}, ctx} ->
            drain_stream_data(ctx, track_alias, stream, decoder, data, objects)

          {:ok, {:stream_event, ^stream, :peer_finished_sending, _metadata}, ctx} ->
            remove_active_data_stream(stream)
            {Enum.reverse(objects), ctx}

          {:ok, _event, ctx} ->
            drain_active_objects(ctx, track_alias, objects)

          {:unknown, _message, ctx} ->
            drain_active_objects(ctx, track_alias, objects)

          {:timeout, ctx} ->
            {Enum.reverse(objects), ctx}
        end
    end
  end

  defp drain_stream_data(ctx, track_alias, stream, decoder, data, objects) do
    case SubgroupDecoder.push(decoder, data) do
      {:ok, decoder, decoded} ->
        put_active_data_stream(track_alias, stream, decoder)
        drain_active_objects(ctx, track_alias, Enum.reverse(decoded) ++ objects)

      {:error, _reason} ->
        {Enum.reverse(objects), ctx}
    end
  end

  defp drain_available_data_streams(ctx, conn, objects) do
    case Transport.accept_stream(ctx, conn, [], 10) do
      {:ok, %{info: %{direction: :unidirectional}} = stream, ctx} ->
        with {:ok, ctx} <- Transport.set_active(ctx, stream, true),
             {:ok, object, ctx} <- receive_object(ctx, stream, %SubgroupDecoder{}) do
          drain_available_data_streams(ctx, conn, [object | objects])
        end

      {:ok, _stream, ctx} ->
        {Enum.reverse(objects), ctx}

      {:error, :timeout, ctx} ->
        {Enum.reverse(objects), ctx}

      {:error, _reason, ctx} ->
        {Enum.reverse(objects), ctx}
    end
  end
end
