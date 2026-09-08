defmodule Membrane.MOQX.LiteRoundtripTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}
  alias Membrane.MOQX.{Session, Sink, Source, TestControlledSource, TestLiteBridge, Track, Unit}
  require Pad

  test "shared Sources isolate abrupt departure and return demand to zero before resubscription" do
    relay = TestLiteBridge.start()
    namespace = ["hermetic", "owners"]
    format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :media), options: [track_name: "media", timescale: 1_000_000])
          |> child(:sink, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            namespace: namespace,
            transport: relay.transport,
            track_demand_events: true
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "media"})

    session =
      start_supervised!(
        {Session,
         endpoint: relay.subscriber_endpoint, protocol: :moq_lite_05, transport: relay.transport}
      )

    branch = fn name ->
      spec =
        child({:source, name}, %Source{
          protocol: :moq_lite_05,
          session: session,
          track: %MOQX.TrackRef{namespace: namespace, track: "media"},
          stream_format: format
        })
        |> child({:collector, name}, Testing.Sink)

      {spec, group: name, crash_group_mode: :temporary}
    end

    subscriber = Testing.Pipeline.start_link_supervised!(spec: branch.(:first))
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 1})

    assert_pipeline_notified(
      publisher,
      :producer,
      {:track_demand, :output,
       %Membrane.MOQX.Event.TrackDemand{active?: true, subscriber_count: 1}}
    )

    Testing.Pipeline.execute_actions(subscriber, spec: branch.(:second))
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 2})

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish, [buffer("both", 1_000_000, true)]}
    )

    assert_sink_buffer(subscriber, {:collector, :first}, %Buffer{payload: "both"})
    assert_sink_buffer(subscriber, {:collector, :second}, %Buffer{payload: "both"})

    Process.exit(Testing.Pipeline.get_child_pid!(subscriber, {:source, :first}), :kill)
    assert_child_terminated(subscriber, {:source, :first})
    assert_pipeline_notified(publisher, :sink, {:subscriber_left, "media", _, 1})

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish, [buffer("survivor", 2_000_000, true)]}
    )

    assert_sink_buffer(subscriber, {:collector, :second}, %Buffer{payload: "survivor"})

    Process.exit(Testing.Pipeline.get_child_pid!(subscriber, {:source, :second}), :kill)
    assert_child_terminated(subscriber, {:source, :second})
    assert_pipeline_notified(publisher, :sink, {:subscriber_left, "media", _, 0})

    assert_pipeline_notified(
      publisher,
      :producer,
      {:track_demand, :output,
       %Membrane.MOQX.Event.TrackDemand{active?: false, subscriber_count: 0}}
    )

    Testing.Pipeline.execute_actions(subscriber, spec: branch.(:third))
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 1})

    assert_pipeline_notified(
      publisher,
      :producer,
      {:track_demand, :output,
       %Membrane.MOQX.Event.TrackDemand{active?: true, subscriber_count: 1}}
    )

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish, [buffer("new", 3_000_000, true)]}
    )

    assert_sink_buffer(subscriber, {:collector, :third}, %Buffer{payload: "new", pts: 3_000_000})
    Testing.Pipeline.terminate(subscriber)
    Session.close(session)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "catalog media deselection cancels demand and permits reselection without losing the catalog" do
    relay = TestLiteBridge.start()
    namespace = ["hermetic", "reselect.hang"]

    format = %Track{
      packaging: "opus",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> child(:framing, %Membrane.MOQX.Hang.Legacy{direction: :encode})
          |> via_in(Pad.ref(:input, :opus), options: [track_name: "opus", timescale: 1_000_000])
          |> child(:sink, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "opus"})

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %Membrane.MOQX.CatalogSource{
            endpoint: relay.subscriber_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(
      subscriber,
      :catalog,
      {:track_available, %Membrane.MOQX.TrackOffer{track_ref: ref} = offer}
    )

    for generation <- 1..2 do
      Testing.Pipeline.execute_actions(subscriber,
        spec:
          get_child(:catalog)
          |> via_out(Pad.ref(:output, :opus), options: [track: ref])
          |> child(:framing, %Membrane.MOQX.Hang.Legacy{direction: :decode})
          |> child(:sink, Testing.Sink)
      )

      assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "opus", _, 1})
      pts = generation * 1_000_000
      packet = buffer(<<generation>>, pts, true)
      Testing.Pipeline.notify_child(publisher, :producer, {:publish, [packet]})

      assert_sink_buffer(subscriber, :sink, %Buffer{payload: <<^generation>>, pts: ^pts})

      Testing.Pipeline.execute_actions(subscriber, remove_children: [:framing, :sink])
      assert_child_terminated(subscriber, :framing)
      assert_child_terminated(subscriber, :sink)
      assert_pipeline_notified(publisher, :sink, {:subscriber_left, "opus", _, 0})
    end

    # A live catalog update after both media selections were removed proves
    # media cancellation did not cancel the catalog's independent subscription.
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)
    assert_pipeline_notified(subscriber, :catalog, {:track_unavailable, ^offer})
    assert Process.alive?(subscriber)
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "HANG Sink catalog selection preserves legacy media and withdraws the finished offer" do
    relay = TestLiteBridge.start()
    namespace = ["hermetic", "hang"]

    format = %Track{
      packaging: "opus",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> child(:framing, %Membrane.MOQX.Hang.Legacy{direction: :encode})
          |> via_in(Pad.ref(:input, :opus), options: [track_name: "opus", timescale: 1_000_000])
          |> child(:sink, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "opus"})

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %Membrane.MOQX.CatalogSource{
            endpoint: relay.subscriber_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(
      subscriber,
      :catalog,
      {:track_available, %Membrane.MOQX.TrackOffer{track_ref: ref} = offer}
    )

    assert ref == %MOQX.TrackRef{namespace: namespace, track: "opus"}
    assert offer.stream_format.packaging == "hang/legacy"

    Testing.Pipeline.execute_actions(subscriber,
      spec:
        get_child(:catalog)
        |> via_out(Pad.ref(:output, :opus), options: [track: ref])
        |> child(:framing, %Membrane.MOQX.Hang.Legacy{direction: :decode})
        |> child(:sink, Testing.Sink)
    )

    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "opus", _, 1})
    packet = buffer(<<0xF8, 0xFF, 0xFE>>, 1_500_000, true)
    Testing.Pipeline.notify_child(publisher, :producer, {:publish, [packet]})
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    receive do
      {Testing.Pipeline, ^subscriber, {:handle_child_notification, {{:buffer, received}, :sink}}} ->
        assert %Buffer{
                 payload: <<0xF8, 0xFF, 0xFE>>,
                 pts: 1_500_000,
                 metadata: %{moqx: %Unit{group_id: 0, object_id: 0, group_end?: true}}
               } = received

      {Testing.Pipeline, ^subscriber, {:handle_element_end_of_stream, {:sink, :input}}} ->
        flunk("EOS arrived before the final HANG packet")
    after
      2_000 -> flunk("missing final HANG packet")
    end

    assert_end_of_stream(subscriber, :sink, :input)
    assert_pipeline_notified(subscriber, :catalog, {:track_unavailable, ^offer})
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "raw Sink and Source preserve media, PTS and group boundaries through immediate EOS" do
    relay = TestLiteBridge.start()
    namespace = ["hermetic", "raw"]
    format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :media), options: [track_name: "media", timescale: 1_000_000])
          |> child(:sink, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "media"})

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            endpoint: relay.subscriber_endpoint,
            protocol: :moq_lite_05,
            track: %MOQX.TrackRef{namespace: namespace, track: "media"},
            stream_format: format,
            transport: relay.transport
          })
          |> child(:sink, Testing.Sink)
      )

    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 1})

    buffers = [
      buffer("first", 1_000_000, false),
      buffer("second", 1_500_000, true),
      buffer("last", 2_000_000, true)
    ]

    Testing.Pipeline.notify_child(publisher, :producer, {:publish, buffers})
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    for {payload, pts, group, object, end?} <- [
          {"first", 1_000_000, 0, 0, false},
          {"second", 1_500_000, 0, 1, true},
          {"last", 2_000_000, 1, 0, true}
        ] do
      receive do
        {Testing.Pipeline, ^subscriber,
         {:handle_child_notification, {{:buffer, received}, :sink}}} ->
          assert %Buffer{
                   payload: ^payload,
                   pts: ^pts,
                   metadata: %{
                     moqx: %Unit{group_id: ^group, object_id: ^object, group_end?: ^end?}
                   }
                 } = received

        {Testing.Pipeline, ^subscriber, {:handle_element_end_of_stream, {:sink, :input}}} ->
          flunk("EOS arrived before #{payload}")
      after
        2_000 -> flunk("missing #{payload}")
      end
    end

    assert_end_of_stream(subscriber, :sink, :input)
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  defp buffer(payload, pts, group_end?),
    do: %Buffer{payload: payload, pts: pts, metadata: %{moqx: %Unit{group_end?: group_end?}}}
end
