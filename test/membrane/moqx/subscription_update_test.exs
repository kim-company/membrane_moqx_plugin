defmodule Membrane.MOQX.SubscriptionUpdateTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.MOQX.{
    CatalogSource,
    Session,
    Sink,
    Source,
    TestControlledSource,
    TestLiteBridge,
    Track,
    TrackOffer
  }

  alias Membrane.{Pad, Testing}
  require Pad
  alias MOQX.Protocol.MOQLite05.{Codec, Messages}
  alias MOQX.Testing.Transport, as: Support
  alias MOQX.Transport

  test "CatalogSource routes updates to the selected pad and reports missing selections without crashing" do
    relay = TestLiteBridge.start()
    namespace = ["updates.hang"]

    format = %Track{
      packaging: "hang/legacy",
      initialization: nil,
      catalog_fields: %{"role" => "audio"},
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :media), options: [track_name: "audio", timescale: 1_000_000])
          |> child(:publisher, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :publisher, {:track_ready, _, "audio"})

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: relay.subscriber_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_available, %TrackOffer{track_ref: track}}
    )

    pad = Pad.ref(:output, :selected)

    Testing.Pipeline.execute_actions(pipeline,
      spec:
        get_child(:catalog)
        |> via_out(pad, options: [track: track])
        |> child(:consumer, Testing.Sink)
    )

    assert_pipeline_notified(publisher, :publisher, {:subscriber_joined, "audio", _, 1})

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish,
       [
         %Membrane.Buffer{
           payload: "before-update",
           pts: 1_000_000,
           metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
         }
       ]}
    )

    assert_sink_stream_format(pipeline, :consumer, %Track{})
    assert_sink_buffer(pipeline, :consumer, %Membrane.Buffer{payload: "before-update"})

    Testing.Pipeline.notify_child(
      pipeline,
      :catalog,
      {:update_subscription, pad, :selected, [priority: 17]}
    )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_source, ^track, {:subscription_update_result, :selected, :ok}}
    )

    missing = Pad.ref(:output, :missing)

    Testing.Pipeline.notify_child(
      pipeline,
      :catalog,
      {:update_subscription, missing, :missing, [priority: 23]}
    )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:subscription_update_result, :missing, {:error, :unknown_selection}}
    )

    Testing.Pipeline.notify_child(
      pipeline,
      :catalog,
      {:update_subscription, pad, :invalid, [priority: 256]}
    )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_source, ^track,
       {:subscription_update_result, :invalid, {:error, :invalid_priority}}}
    )

    Testing.Pipeline.notify_child(
      pipeline,
      :catalog,
      {:update_subscription, pad, :again, [priority: 23]}
    )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_source, ^track, {:subscription_update_result, :again, :ok}}
    )

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish,
       [
         %Membrane.Buffer{
           payload: "after-update",
           pts: 2_000_000,
           metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
         }
       ]}
    )

    assert_sink_buffer(pipeline, :consumer, %Membrane.Buffer{payload: "after-update"})
    Testing.Pipeline.execute_actions(pipeline, remove_children: :consumer)
    assert_pipeline_notified(publisher, :publisher, {:subscriber_left, "audio", _, 0})

    Testing.Pipeline.notify_child(
      pipeline,
      :catalog,
      {:update_subscription, pad, :removed, [priority: 17]}
    )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:subscription_update_result, :removed, {:error, :unknown_selection}}
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "standalone and shared Sources report correlated local update results without losing their subscription" do
    for mode <- [:standalone, :shared] do
      peer = start_peer(1)
      transport = {Support, network: peer.network, profile: :moq_lite_05}

      session =
        if mode == :shared do
          {:ok, session} =
            Session.start_link(
              endpoint: peer.endpoint,
              protocol: :moq_lite_05,
              transport: transport
            )

          session
        end

      pipeline =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:source, %Source{
              endpoint: peer.endpoint,
              session: session,
              protocol: :moq_lite_05,
              transport: transport,
              track: %MOQX.TrackRef{namespace: ["updates"], track: "video"},
              stream_format: %Track{packaging: "opus", initialization: nil}
            })
            |> child(:consumer, Testing.Sink)
        )

      assert_sink_stream_format(pipeline, :consumer, %Track{})

      Testing.Pipeline.notify_child(
        pipeline,
        :source,
        {:update_subscription, :change, [priority: 17]}
      )

      assert_pipeline_notified(pipeline, :source, {:subscription_update_result, :change, :ok})

      assert_wire_update(peer, 0, %Messages.SubscribeUpdate{
        subscriber_priority: 17,
        subscriber_ordered: false,
        subscriber_max_latency: 0
      })

      Testing.Pipeline.notify_child(
        pipeline,
        :source,
        {:update_subscription, :invalid, [priority: 256]}
      )

      assert_pipeline_notified(
        pipeline,
        :source,
        {:subscription_update_result, :invalid, {:error, :invalid_priority}}
      )

      Testing.Pipeline.notify_child(
        pipeline,
        :source,
        {:update_subscription, :next, [priority: 23]}
      )

      assert_pipeline_notified(pipeline, :source, {:subscription_update_result, :next, :ok})

      assert_wire_update(peer, 0, %Messages.SubscribeUpdate{
        subscriber_priority: 23,
        subscriber_ordered: false,
        subscriber_max_latency: 0
      })

      assert :ok = Testing.Pipeline.terminate(pipeline)
      if session, do: Session.close(session)
      send(peer.task.pid, :stop)
      assert :ok = Task.await(peer.task)
    end
  end

  test "Sources distinguish local admission from typed peer update rejection and acknowledgement" do
    for mode <- [:standalone, :shared] do
      peer = start_draft16_peer()
      transport = {Support, network: peer.network, profile: :draft_16}

      session =
        if mode == :shared do
          {:ok, session} =
            Session.start_link(endpoint: peer.endpoint, protocol: :draft_16, transport: transport)

          session
        end

      track = %MOQX.TrackRef{namespace: ["updates"], track: "video"}

      pipeline =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:source, %Source{
              endpoint: peer.endpoint,
              session: session,
              protocol: :draft_16,
              transport: transport,
              track: track,
              stream_format: %Track{packaging: "opus", initialization: nil}
            })
            |> child(:consumer, Testing.Sink)
        )

      assert_sink_stream_format(pipeline, :consumer, %Track{})

      Testing.Pipeline.notify_child(
        pipeline,
        :source,
        {:update_subscription, :rejected, [priority: 17]}
      )

      assert_pipeline_notified(pipeline, :source, {:subscription_update_result, :rejected, :ok})

      assert_pipeline_notified(
        pipeline,
        :source,
        {:subscription_update_failed, ^track,
         %MOQX.ProtocolError{operation: :update_subscription, code: 8}}
      )

      Testing.Pipeline.notify_child(
        pipeline,
        :source,
        {:update_subscription, :accepted, [priority: 23]}
      )

      assert_pipeline_notified(pipeline, :source, {:subscription_update_result, :accepted, :ok})
      assert_pipeline_notified(pipeline, :source, {:subscription_updated, ^track, []})
      assert :ok = Testing.Pipeline.terminate(pipeline)
      if session, do: Session.close(session)
      send(peer.task.pid, :stop)
      assert :ok = Task.await(peer.task)
    end
  end

  test "draft-16 update rejection and acknowledgement reach the owner without ending its subscription" do
    peer = start_draft16_peer()

    {:ok, session} =
      Session.start_link(
        endpoint: peer.endpoint,
        protocol: :draft_16,
        transport: {Support, network: peer.network, profile: :draft_16}
      )

    {:ok, subscription} =
      Session.subscribe(session, %MOQX.TrackRef{namespace: ["updates"], track: "video"})

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.SubscriptionAccepted{subscription: ^subscription}},
                   2_000

    assert :ok = Session.update_subscription(session, subscription, priority: 17)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.SubscriptionUpdateFailed{
                      subscription: ^subscription,
                      error: %MOQX.ProtocolError{operation: :update_subscription, code: 8}
                    }},
                   2_000

    assert :ok = Session.update_subscription(session, subscription, priority: 23)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.SubscriptionUpdated{subscription: ^subscription}},
                   2_000

    assert :ok = Session.unsubscribe(session, subscription)
    assert :ok = Session.close(session)
    send(peer.task.pid, :stop)
    assert :ok = Task.await(peer.task)
  end

  test "only the subscription owner updates its wire parameters and invalid updates preserve siblings" do
    peer = start_peer()

    {:ok, session} =
      Session.start_link(
        endpoint: peer.endpoint,
        protocol: :moq_lite_05,
        transport: {Support, network: peer.network, profile: :moq_lite_05}
      )

    track = %MOQX.TrackRef{namespace: ["updates"], track: "video"}
    {:ok, first} = Session.subscribe(session, track)
    {:ok, second} = Session.subscribe(session, track)

    for subscription <- [first, second] do
      assert_receive {:moqx_session, ^session,
                      %MOQX.Event.SubscriptionAccepted{subscription: ^subscription}},
                     2_000
    end

    foreign = Task.async(fn -> Session.update_subscription(session, first, priority: 99) end)
    assert {:error, :not_subscription_owner} = Task.await(foreign)

    assert {:error, :invalid_priority} =
             Session.update_subscription(session, first, priority: 256)

    filter = %MOQX.SubscriptionFilter{type: :absolute_range, start_location: {3, 0}, end_group: 7}

    assert :ok =
             Session.update_subscription(session, first,
               priority: 17,
               group_order: :ascending,
               delivery_timeout: 42,
               filter: filter
             )

    assert_wire_update(peer, 0, %Messages.SubscribeUpdate{
      subscriber_priority: 17,
      subscriber_ordered: true,
      subscriber_max_latency: 42,
      group_start: 3,
      group_end: 7
    })

    assert :ok = Session.unsubscribe(session, first)

    assert {:error, :unknown_subscription} =
             Session.update_subscription(session, first, priority: 18)

    assert :ok = Session.update_subscription(session, second, priority: 23)

    assert_wire_update(peer, 1, %Messages.SubscribeUpdate{
      subscriber_priority: 23,
      subscriber_ordered: false,
      subscriber_max_latency: 0
    })

    assert :ok = Session.close(session)
    send(peer.task.pid, :stop)
    assert :ok = Task.await(peer.task)
  end

  defp assert_wire_update(peer, id, update) do
    send(peer.task.pid, {:read_update, id, update})
    assert_receive {:wire_update, ^id, ^update}, 2_000
  end

  defp start_peer(count \\ 2) do
    {:ok, network} = Support.start_network()
    parent = self()

    task =
      Task.async(fn ->
        {:ok, ctx} = Transport.new(Support, network: network, profile: :moq_lite_05)
        {:ok, listener, ctx} = Transport.listen(ctx, 0)
        {:ok, {_ip, port}} = Transport.local_address(ctx, listener)
        send(parent, {:listening, port})
        {:ok, conn, ctx} = Transport.accept(ctx, listener, [], 2_000)
        {:ok, conn, ctx} = Transport.handshake(ctx, conn, 2_000)
        {:ok, setup, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)
        setup_bytes = <<1, Codec.encode_setup(%Messages.Setup{path: "/", role: :both})::binary>>
        {:ok, ^setup_bytes, ctx} = Transport.recv_stream(ctx, setup, byte_size(setup_bytes))

        {ctx, streams} =
          Enum.reduce(0..(count - 1), {ctx, %{}}, fn id, {ctx, streams} ->
            {:ok, metadata, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)
            {:ok, stream, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)

            track =
              <<6,
                Codec.encode_track(%Messages.Track{
                  broadcast_path: "updates",
                  track_name: "video"
                })::binary>>

            {:ok, ^track, ctx} = Transport.recv_stream(ctx, metadata, byte_size(track))

            request =
              <<2,
                Codec.encode_subscribe(%Messages.Subscribe{
                  subscribe_id: id,
                  broadcast_path: "updates",
                  track_name: "video",
                  subscriber_priority: 128
                })::binary>>

            {:ok, ^request, ctx} = Transport.recv_stream(ctx, stream, byte_size(request))

            info =
              Codec.encode_track_info(%Messages.TrackInfo{
                timescale: 1_000_000,
                publisher_priority: 17,
                publisher_ordered: false,
                publisher_max_latency: 0
              })

            {:ok, _, ctx} = Transport.send_stream(ctx, metadata, info, finish: true)

            {:ok, _, ctx} =
              Transport.send_stream(
                ctx,
                stream,
                Codec.encode_subscribe_response(%Messages.SubscribeOk{group: 0})
              )

            {ctx, Map.put(streams, id, stream)}
          end)

        read_updates(ctx, streams, parent)
      end)

    assert_receive {:listening, port}, 2_000
    %{task: task, network: network, endpoint: "moql://localhost:#{port}"}
  end

  defp start_draft16_peer do
    {:ok, network} = Support.start_network()
    parent = self()

    task =
      Task.async(fn ->
        {:ok, ctx} = Transport.new(Support, network: network, profile: :draft_16)
        {:ok, listener, ctx} = Transport.listen(ctx, 0)
        {:ok, {_ip, port}} = Transport.local_address(ctx, listener)
        send(parent, {:listening, port})
        {:ok, conn, ctx} = Transport.accept(ctx, listener, [], 2_000)
        {:ok, conn, ctx} = Transport.handshake(ctx, conn, 2_000)
        {:ok, control, ctx} = Transport.accept_stream(ctx, conn, [], 2_000)
        {0x20, _setup, ctx} = control_frame(ctx, control)
        {:ok, _, ctx} = Transport.send_stream(ctx, control, <<0x21, 0, 1, 0, 0x15, 0, 1, 20>>)
        {3, _subscribe, ctx} = control_frame(ctx, control)
        {:ok, _, ctx} = Transport.send_stream(ctx, control, <<4, 0, 3, 0, 7, 0>>)
        {2, <<2, _rest::binary>>, ctx} = control_frame(ctx, control)
        {:ok, _, ctx} = Transport.send_stream(ctx, control, <<5, 0, 6, 2, 8, 0, 2, "no">>)
        {2, <<4, _rest::binary>>, ctx} = control_frame(ctx, control)
        {:ok, _, _ctx} = Transport.send_stream(ctx, control, <<7, 0, 2, 4, 0>>)

        receive do
          :stop -> :ok
        after
          5_000 -> raise "draft16 update peer timed out"
        end
      end)

    assert_receive {:listening, port}, 2_000
    %{task: task, network: network, endpoint: "moqt://localhost:#{port}"}
  end

  defp control_frame(ctx, stream) do
    {:ok, <<type, size::16>>, ctx} = Transport.recv_stream(ctx, stream, 3)
    {:ok, body, ctx} = Transport.recv_stream(ctx, stream, size)
    {type, body, ctx}
  end

  defp read_updates(ctx, streams, parent) do
    receive do
      {:read_update, id, update} ->
        expected = Codec.encode_subscribe_update(update)

        {:ok, bytes, ctx} =
          Transport.recv_stream(ctx, Map.fetch!(streams, id), byte_size(expected))

        assert bytes == expected
        {:ok, update} = Codec.decode_subscribe_update(bytes)
        send(parent, {:wire_update, id, update})
        read_updates(ctx, streams, parent)

      :stop ->
        :ok
    after
      5_000 -> raise "subscription update peer timed out"
    end
  end
end
