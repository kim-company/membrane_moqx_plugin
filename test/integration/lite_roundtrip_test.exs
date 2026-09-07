defmodule Membrane.MOQX.Integration.LiteRoundtripTest do
  use ExUnit.Case, async: false
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.Hang.Legacy

  alias Membrane.MOQX.{
    CatalogSource,
    Session,
    Sink,
    Source,
    TestControlledSource,
    Track,
    TrackOffer,
    Unit
  }

  alias Membrane.Testing
  require Pad

  @moduletag :integration
  @timeout 15_000

  test "HANG catalog selection composes with legacy media framing over native QUIC" do
    endpoint = System.fetch_env!("MOQX_LITE_ENDPOINT")
    ca = System.get_env("MOQX_LITE_CA_FILE", "/etc/ssl/cert.pem")
    connection = [verify: :verify_peer, cacertfile: ca]
    namespace = ["membrane-hang", "run-#{System.system_time(:nanosecond)}"]

    format = %Track{
      packaging: "opus",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> child(:framing, %Legacy{direction: :encode})
          |> via_in(Pad.ref(:input, :opus), options: [track_name: "opus", timescale: 1_000_000])
          |> child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            connect_options: connection
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "opus"}, @timeout)

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            connect_options: connection
          })
      )

    assert_pipeline_notified(
      subscriber,
      :catalog,
      {:track_available,
       %TrackOffer{track_ref: ref, stream_format: %Track{packaging: "hang/legacy"}} = offer},
      @timeout
    )

    assert ref == %MOQX.TrackRef{namespace: namespace, track: "opus"}

    spec =
      get_child(:catalog)
      |> via_out(Pad.ref(:output, :opus), options: [track: ref])
      |> child(:framing, %Legacy{direction: :decode})
      |> child(:sink, Testing.Sink)

    Testing.Pipeline.execute_actions(subscriber, spec: spec)
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "opus", _, 1}, @timeout)

    buffer = %Buffer{
      payload: <<0xF8, 0xFF, 0xFE>>,
      pts: 1_500_000,
      metadata: %{moqx: %Unit{group_end?: true}}
    }

    Testing.Pipeline.notify_child(publisher, :producer, {:publish, [buffer]})
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    receive do
      {Testing.Pipeline, ^subscriber, {:handle_child_notification, {{:buffer, received}, :sink}}} ->
        assert %Buffer{
                 payload: <<0xF8, 0xFF, 0xFE>>,
                 pts: 1_500_000,
                 metadata: %{moqx: %Unit{group_end?: true}}
               } = received

      {Testing.Pipeline, ^subscriber, {:handle_element_end_of_stream, {:sink, :input}}} ->
        flunk("EOS arrived before the HANG media payload")
    after
      @timeout -> flunk("HANG media not delivered")
    end

    assert_end_of_stream(subscriber, :sink, :input, @timeout)
    assert_pipeline_notified(subscriber, :catalog, {:track_unavailable, ^offer}, @timeout)
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
  end

  test "a complete plugin Lite roundtrip delivers the final payload and PTS before EOS" do
    endpoint = System.fetch_env!("MOQX_LITE_ENDPOINT")
    ca = System.get_env("MOQX_LITE_CA_FILE", "/etc/ssl/cert.pem")
    connection = [verify: :verify_peer, cacertfile: ca]
    namespace = ["membrane-roundtrip", "run-#{System.system_time(:nanosecond)}"]
    ref = %MOQX.TrackRef{namespace: namespace, track: "events"}
    format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            profile: :none,
            namespace: namespace,
            connect_options: connection,
            inbound_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(publisher, :sink, {:publication_ready, ^namespace}, @timeout)

    spec =
      child(:producer, %TestControlledSource{stream_format: format})
      |> via_in(Pad.ref(:input, :events), options: [track_name: "events", timescale: 1_000_000])
      |> get_child(:sink)

    Testing.Pipeline.execute_actions(publisher, spec: spec)
    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "events"}, @timeout)

    session =
      start_supervised!(
        {Session, endpoint: endpoint, protocol: :moq_lite_05, connect_options: connection}
      )

    {:ok, discovery} = Session.discover(session, "")
    path = Enum.join(namespace, "/")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: ^path}},
                   @timeout

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            track: ref,
            session: session,
            stream_format: format,
            connect_options: connection
          })
          |> child(:sink, Testing.Sink)
      )

    assert_pipeline_notified(
      publisher,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request},
      @timeout
    )

    assert request.track == ref
    Testing.Pipeline.notify_child(publisher, :sink, {:accept_subscription, request})
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "events", _, 1}, @timeout)

    buffer = %Buffer{
      payload: "last-payload",
      pts: 1_500_000,
      metadata: %{moqx: %Unit{group_end?: true}}
    }

    Testing.Pipeline.notify_child(publisher, :producer, {:publish, [buffer]})
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    receive do
      {Testing.Pipeline, ^subscriber, {:handle_child_notification, {{:buffer, received}, :sink}}} ->
        assert %Buffer{
                 payload: "last-payload",
                 pts: 1_500_000,
                 metadata: %{moqx: %Unit{group_end?: true}}
               } = received

      {Testing.Pipeline, ^subscriber, {:handle_element_end_of_stream, {:sink, :input}}} ->
        flunk("EOS arrived before the final payload")
    after
      @timeout -> flunk("final payload not delivered")
    end

    assert_end_of_stream(subscriber, :sink, :input, @timeout)
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
  end
end
