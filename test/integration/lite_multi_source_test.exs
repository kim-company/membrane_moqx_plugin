defmodule Membrane.MOQX.Integration.LiteMultiSourceTest do
  use ExUnit.Case, async: false
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}
  alias Membrane.MOQX.Event.TrackDemand
  alias Membrane.MOQX.{Session, Sink, Source, TestControlledSource, Track, Unit}
  require Pad

  @moduletag :integration
  @timeout 45_000

  @tag timeout: 180_000
  test "native relay preserves shared Source ownership, aggregate demand and resubscription" do
    endpoint = System.fetch_env!("MOQX_LITE_ENDPOINT")

    connection = [
      verify: :verify_peer,
      cacertfile: System.get_env("MOQX_LITE_CA_FILE", "/etc/ssl/cert.pem")
    ]

    namespace = ["membrane-owners", "run-#{System.system_time(:nanosecond)}"]
    format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :media),
            options: [track_name: "media", timescale: 1_000_000, retention: :all]
          )
          |> child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            namespace: namespace,
            connect_options: connection,
            track_demand_events: true
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "media"}, @timeout)

    session =
      start_supervised!(
        {Session, endpoint: endpoint, protocol: :moq_lite_05, connect_options: connection}
      )

    {:ok, discovery} = Session.discover(session, "")
    path = Enum.join(namespace, "/")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: ^path}},
                   @timeout

    branch = fn name, options ->
      spec =
        child({:source, name}, %Source{
          protocol: :moq_lite_05,
          session: session,
          track: %MOQX.TrackRef{namespace: namespace, track: "media"},
          stream_format: format,
          subscription_options: options
        })
        |> child({:collector, name}, Testing.Sink)

      {spec, group: name, crash_group_mode: :temporary}
    end

    subscriber = Testing.Pipeline.start_link_supervised!(spec: branch.(:first, []))
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 1}, @timeout)

    assert_pipeline_notified(
      publisher,
      :producer,
      {:track_demand, :output, %TrackDemand{active?: true}},
      @timeout
    )

    # The real relay aggregates downstream subscriptions into one upstream
    # subscription; the Sink does not count viewers behind that relay.
    Testing.Pipeline.execute_actions(subscriber, spec: branch.(:second, []))
    publish(publisher, "both", 1_000_000)

    assert_sink_buffer(
      subscriber,
      {:collector, :first},
      %Buffer{payload: "both", pts: 1_000_000},
      @timeout
    )

    assert_sink_buffer(
      subscriber,
      {:collector, :second},
      %Buffer{payload: "both", pts: 1_000_000},
      @timeout
    )

    Process.exit(Testing.Pipeline.get_child_pid!(subscriber, {:source, :first}), :kill)
    assert_child_terminated(subscriber, {:source, :first}, @timeout)
    publish(publisher, "survivor", 2_000_000)

    assert_sink_buffer(
      subscriber,
      {:collector, :second},
      %Buffer{payload: "survivor", pts: 2_000_000},
      @timeout
    )

    Process.exit(Testing.Pipeline.get_child_pid!(subscriber, {:source, :second}), :kill)
    assert_child_terminated(subscriber, {:source, :second}, @timeout)
    assert_pipeline_notified(publisher, :sink, {:subscriber_left, "media", _, 0}, @timeout)

    assert_pipeline_notified(
      publisher,
      :producer,
      {:track_demand, :output, %TrackDemand{active?: false}},
      @timeout
    )

    options = [filter: %MOQX.SubscriptionFilter{type: :absolute_start, start_location: {2, 0}}]
    Testing.Pipeline.execute_actions(subscriber, spec: branch.(:third, options))
    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "media", _, 1}, @timeout)

    assert_pipeline_notified(
      publisher,
      :producer,
      {:track_demand, :output, %TrackDemand{active?: true}},
      @timeout
    )

    publish(publisher, "new", 3_000_000)
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    receive do
      {Testing.Pipeline, ^subscriber,
       {:handle_child_notification, {{:buffer, received}, {:collector, :third}}}} ->
        assert %Buffer{
                 payload: "new",
                 pts: 3_000_000,
                 metadata: %{moqx: %Unit{group_id: 2, object_id: 0, group_end?: true}}
               } = received

      {Testing.Pipeline, ^subscriber,
       {:handle_element_end_of_stream, {{:collector, :third}, :input}}} ->
        flunk("EOS preceded resubscribed media")
    after
      @timeout -> flunk("resubscribed Source did not receive final media")
    end

    assert_end_of_stream(subscriber, {:collector, :third}, :input, @timeout)
    Testing.Pipeline.terminate(subscriber)
    Session.close(session)
    Testing.Pipeline.terminate(publisher)
  end

  defp publish(publisher, payload, pts) do
    buffer = %Buffer{payload: payload, pts: pts, metadata: %{moqx: %Unit{group_end?: true}}}
    Testing.Pipeline.notify_child(publisher, :producer, {:publish, [buffer]})
  end
end
