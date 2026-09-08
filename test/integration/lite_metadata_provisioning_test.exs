defmodule Membrane.MOQX.Integration.LiteMetadataProvisioningTest do
  use ExUnit.Case, async: false
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}
  alias Membrane.MOQX.{Session, Sink, Source, TestControlledSource, Track, Unit}
  require Pad

  @moduletag :integration
  @timeout 15_000

  test "absent-track metadata provisioning requires separate admission and drains final media" do
    context = start_publication()
    %{publisher: publisher, namespace: namespace, format: format} = context
    subscriber = start_subscriber(context)

    assert_pipeline_notified(publisher, :sink, {:track_metadata_requested, request}, @timeout)
    assert request.track == %MOQX.TrackRef{namespace: namespace, track: "media"}

    Testing.Pipeline.execute_actions(publisher,
      spec:
        child(:producer, %TestControlledSource{stream_format: format})
        |> via_in(Pad.ref(:input, :media), options: [track_name: "media", timescale: 1_000_000])
        |> get_child(:sink)
    )

    assert_pipeline_notified(
      publisher,
      :sink,
      {:track_metadata_request_done,
       %MOQX.Event.PublicationTrackRequestDone{request: ^request, reason: :registered, error: nil}},
      @timeout
    )

    assert_pipeline_notified(publisher, :sink, {:subscription_requested, admission}, @timeout)
    assert admission.track == request.track
    refute_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, _}, 0)
    Testing.Pipeline.notify_child(publisher, :sink, {:accept_subscription, admission})
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 1}, @timeout)

    buffer = %Buffer{
      payload: "last-provisioned-payload",
      pts: 3_000_000,
      metadata: %{moqx: %Unit{group_end?: true}}
    }

    Testing.Pipeline.notify_child(publisher, :producer, {:publish, [buffer]})
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    receive do
      {Testing.Pipeline, ^subscriber,
       {:handle_child_notification, {{:buffer, received}, :consumer}}} ->
        assert %Buffer{
                 payload: "last-provisioned-payload",
                 pts: 3_000_000,
                 metadata: %{moqx: %Unit{group_end?: true}}
               } = received

      {Testing.Pipeline, ^subscriber, {:handle_element_end_of_stream, {:consumer, :input}}} ->
        flunk("EOS preceded the provisioned track's final payload")
    after
      @timeout -> flunk("provisioned track did not deliver final media")
    end

    assert_end_of_stream(subscriber, :consumer, :input, @timeout)
    finish(context, subscriber)
  end

  test "explicit metadata rejection reports Source failure and preserves discovery for teardown" do
    context = start_publication()
    %{publisher: publisher} = context
    subscriber = start_subscriber(context)
    assert_pipeline_notified(publisher, :sink, {:track_metadata_requested, request}, @timeout)

    Testing.Pipeline.notify_child(
      publisher,
      :sink,
      {:reject_track_request, request,
       %MOQX.SubscriptionRejection{code: :track_does_not_exist, reason: "not available"}}
    )

    assert_pipeline_notified(
      publisher,
      :sink,
      {:track_metadata_request_done,
       %MOQX.Event.PublicationTrackRequestDone{request: ^request, reason: :rejected}},
      @timeout
    )

    ref = request.track

    assert_pipeline_notified(
      subscriber,
      :source,
      {:subscription_failed, ^ref, %MOQX.ProtocolError{protocol: :moq_lite_05}},
      @timeout
    )

    assert_child_terminated(subscriber, :source, @timeout)
    assert Process.alive?(subscriber)
    assert Process.alive?(publisher)
    finish(context, subscriber)
  end

  test "unanswered metadata demand times out without implicit track registration or admission" do
    context = start_publication(track_metadata_timeout: 100)
    %{publisher: publisher} = context
    subscriber = start_subscriber(context)
    assert_pipeline_notified(publisher, :sink, {:track_metadata_requested, request}, @timeout)
    assert request.timeout_ms == 100

    assert_pipeline_notified(
      publisher,
      :sink,
      {:track_metadata_request_done,
       %MOQX.Event.PublicationTrackRequestDone{request: ^request, reason: :timed_out}},
      @timeout
    )

    ref = request.track

    assert_pipeline_notified(
      subscriber,
      :source,
      {:subscription_failed, ^ref, %MOQX.ProtocolError{protocol: :moq_lite_05}},
      @timeout
    )

    assert_child_terminated(subscriber, :source, @timeout)
    refute_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, _}, 0)
    refute_pipeline_notified(publisher, :sink, {:track_ready, _, "media"}, 0)
    assert Process.alive?(subscriber)
    assert Process.alive?(publisher)
    finish(context, subscriber)
  end

  defp start_publication(options \\ []) do
    endpoint = System.fetch_env!("MOQX_LITE_ENDPOINT")

    connection = [
      verify: :verify_peer,
      cacertfile: System.get_env("MOQX_LITE_CA_FILE", "/etc/ssl/cert.pem")
    ]

    namespace = ["membrane-metadata", "run-#{System.system_time(:nanosecond)}"]
    format = %Track{packaging: "application/example", initialization: nil}

    sink =
      struct!(
        Sink,
        Keyword.merge(
          [
            endpoint: endpoint,
            protocol: :moq_lite_05,
            namespace: namespace,
            connect_options: connection,
            missing_track_metadata: :controlled,
            inbound_subscriptions: :controlled,
            track_metadata_timeout: 5_000
          ],
          options
        )
      )

    publisher = Testing.Pipeline.start_link_supervised!(spec: child(:sink, sink))
    assert_pipeline_notified(publisher, :sink, {:publication_ready, ^namespace}, @timeout)

    session =
      start_supervised!(
        {Session, endpoint: endpoint, protocol: :moq_lite_05, connect_options: connection}
      )

    {:ok, discovery} = Session.discover(session, "")
    path = Enum.join(namespace, "/")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: ^path}},
                   @timeout

    %{
      publisher: publisher,
      session: session,
      discovery: discovery,
      namespace: namespace,
      path: path,
      format: format
    }
  end

  defp start_subscriber(context) do
    spec =
      child(:source, %Source{
        protocol: :moq_lite_05,
        session: context.session,
        track: %MOQX.TrackRef{namespace: context.namespace, track: "media"},
        stream_format: context.format
      })
      |> child(:consumer, Testing.Sink)

    Testing.Pipeline.start_link_supervised!(
      spec: {spec, group: :media, crash_group_mode: :temporary}
    )
  end

  defp finish(context, subscriber) do
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(context.publisher)
    %{session: session, discovery: discovery, path: path} = context

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastWithdrawn{discovery: ^discovery, path: ^path}},
                   @timeout

    assert :ok = Session.cancel_discovery(session, discovery)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.DiscoveryDone{discovery: ^discovery, reason: :cancelled}},
                   @timeout

    assert :ok = Session.close(session)
  end
end
