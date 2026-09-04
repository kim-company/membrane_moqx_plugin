defmodule Membrane.MOQX.TestPublisher do
  @moduledoc false

  alias MOQX.Protocol.MOQTDraft14.{Codec, Messages}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  @timeout 2_000
  @close_timeout 10_000

  defstruct [:task, :network, :endpoint]

  def start(%MOQX.TrackRef{} = track, objects) do
    start_many([{track, objects}])
  end

  def start_interleaved(%MOQX.TrackRef{} = track, subgroup_objects) do
    start_many([{:interleaved, track, subgroup_objects}])
  end

  def start_many(entries) when is_list(entries) and entries != [] do
    {:ok, network} = Support.start_network()
    parent = self()
    task = Task.async(fn -> publish(parent, network, entries) end)

    receive do
      {:publisher_ready, port} ->
        %__MODULE__{
          task: task,
          network: network,
          endpoint: "moqt://localhost:#{port}"
        }
    after
      @timeout -> raise "test publisher did not start"
    end
  end

  def transport(%__MODULE__{network: network}) do
    {Support, network: network, profile: :draft_14}
  end

  def await_shutdown(%__MODULE__{task: task}) do
    case Task.yield(task, @timeout) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:ok, {:error, reason, _context}} -> {:error, reason}
      nil -> {:error, :publisher_shutdown_timeout}
    end
  end

  defp publish(parent, network, entries) do
    with {:ok, ctx} <- Transport.new(Support, network: network, profile: :draft_14),
         {:ok, listener, ctx} <- Transport.listen(ctx, 0),
         {:ok, {_ip, port}} <- Transport.local_address(ctx, listener) do
      send(parent, {:publisher_ready, port})
      serve(ctx, listener, entries)
    end
  end

  defp serve(ctx, listener, entries) do
    with {:ok, conn, ctx} <- Transport.accept(ctx, listener, [], @timeout),
         {:ok, conn, ctx} <- Transport.handshake(ctx, conn, @timeout),
         {:ok, control, ctx} <- Transport.accept_stream(ctx, conn, [], @timeout),
         {:ok, ctx} <- receive_client_setup(ctx, control),
         {:ok, ctx} <- send_server_setup(ctx, control),
         {:ok, ctx} <- serve_subscriptions(ctx, conn, control, entries),
         {:ok, _event, _ctx} <- receive_connection_close(ctx, conn) do
      :ok
    end
  end

  defp serve_subscriptions(ctx, _conn, _control, []), do: {:ok, ctx}

  defp serve_subscriptions(
         ctx,
         conn,
         control,
         [{:interleaved, track, subgroup_objects} | rest]
       ) do
    with {:ok, subscribe, ctx} <- receive_subscribe(ctx, control),
         :ok <- validate_track(subscribe, track),
         {:ok, ctx} <- accept_subscription(ctx, control, subscribe.request_id),
         {:ok, ctx} <-
           send_interleaved_subgroups(ctx, conn, subscribe.request_id, subgroup_objects),
         {:ok, ctx} <- finish_subscription(ctx, control, subscribe.request_id, 2) do
      serve_subscriptions(ctx, conn, control, rest)
    end
  end

  defp serve_subscriptions(ctx, conn, control, [{track, objects} | rest]) do
    with {:ok, subscribe, ctx} <- receive_subscribe(ctx, control),
         :ok <- validate_track(subscribe, track),
         {:ok, ctx} <- accept_subscription(ctx, control, subscribe.request_id),
         {:ok, ctx} <- send_objects(ctx, conn, subscribe.request_id, objects),
         {:ok, ctx} <- finish_subscription(ctx, control, subscribe.request_id, length(objects)) do
      serve_subscriptions(ctx, conn, control, rest)
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
    setup = <<0x21, 0, 9, 0xC0000000FF00000E::64, 0>>

    case Transport.send_stream(ctx, control, setup) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:server_setup_failed, other}}
    end
  end

  defp receive_subscribe(ctx, control) do
    with {:ok, 0x03, payload, ctx} <- receive_control_frame(ctx, control),
         {:ok, subscribe} <- Codec.decode_subscribe(payload) do
      {:ok, subscribe, ctx}
    else
      other -> {:error, {:unexpected_subscribe, other}}
    end
  end

  defp validate_track(subscribe, track) do
    if subscribe.track_namespace == track.namespace and subscribe.track_name == track.track do
      :ok
    else
      {:error, {:unexpected_track, subscribe}}
    end
  end

  defp accept_subscription(ctx, control, request_id) do
    message =
      Codec.encode(%Messages.SubscribeOk{
        request_id: request_id,
        track_alias: request_id,
        expires: 0,
        group_order: :ascending,
        largest_location: nil,
        params: %{}
      })

    case Transport.send_stream(ctx, control, message) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:subscribe_accept_failed, other}}
    end
  end

  defp send_objects(ctx, conn, track_alias, objects) do
    Enum.reduce_while(objects, {:ok, ctx}, fn object, {:ok, ctx} ->
      with {:ok, stream, ctx} <-
             Transport.open_stream(ctx, conn, direction: :unidirectional),
           {:ok, _send, ctx} <-
             Transport.send_stream(
               ctx,
               stream,
               Codec.encode_subgroup(track_alias, object),
               finish: true
             ) do
        {:cont, {:ok, ctx}}
      else
        {:error, reason, ctx} -> {:halt, {:error, reason, ctx}}
      end
    end)
  end

  defp send_interleaved_subgroups(
         ctx,
         conn,
         track_alias,
         [{first, second}, other]
       ) do
    first_bytes = subgroup_bytes(track_alias, first, true)
    second_bytes = subgroup_bytes(track_alias, second, true)
    other_bytes = subgroup_bytes(track_alias, other, false)
    first_continuation = subgroup_object_bytes(second_bytes)

    with {:ok, first_stream, ctx} <-
           Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, second_stream, ctx} <-
           Transport.open_stream(ctx, conn, direction: :unidirectional),
         {:ok, _send, ctx} <- Transport.send_stream(ctx, first_stream, first_bytes),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, second_stream, other_bytes, finish: true),
         :ok <- wait_for_peer_to_process_subgroup_end(),
         {:ok, _send, ctx} <-
           Transport.send_stream(ctx, first_stream, first_continuation, finish: true),
         :ok <- wait_for_peer_to_process_subgroup_end() do
      {:ok, ctx}
    end
  end

  defp wait_for_peer_to_process_subgroup_end do
    receive do
    after
      50 -> :ok
    end
  end

  defp subgroup_bytes(track_alias, object, end_of_group?) do
    <<type, rest::binary>> = Codec.encode_subgroup(track_alias, object)
    type = if end_of_group?, do: Bitwise.bor(type, 0x08), else: type
    <<type, rest::binary>>
  end

  defp subgroup_object_bytes(bytes) do
    {:ok, _header, object_bytes} = Codec.decode_subgroup_header(bytes)
    object_bytes
  end

  defp finish_subscription(ctx, control, request_id, stream_count) do
    message =
      Codec.encode(%Messages.PublishDone{
        request_id: request_id,
        status_code: 2,
        stream_count: stream_count,
        reason_phrase: "track ended"
      })

    case Transport.send_stream(ctx, control, message) do
      {:ok, _send, ctx} -> {:ok, ctx}
      other -> {:error, {:publish_done_failed, other}}
    end
  end

  defp receive_control_frame(ctx, control) do
    with {:ok, <<type, length::16>>, ctx} <- Transport.recv_stream(ctx, control, 3),
         {:ok, payload, ctx} <- Transport.recv_stream(ctx, control, length) do
      {:ok, type, payload, ctx}
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
