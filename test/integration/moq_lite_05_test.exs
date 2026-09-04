defmodule Membrane.MOQX.Integration.MOQLite05Test do
  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.{Sink, TestControlledSource, Track, Unit}
  alias Membrane.Testing

  require Pad

  @moduletag :integration
  @moduletag :moq_lite_05
  @timeout 45_000

  test "preserves aggregate demand through departure and resubscription over native QUIC" do
    namespace = ["membrane-moqx", "lite-#{System.unique_integer([:positive])}"]
    track_name = "opus"
    track_ref = %MOQX.TrackRef{namespace: namespace, track: track_name}
    stream_format = %Track{packaging: "opus", initialization: nil}

    spec =
      child(:source, %TestControlledSource{stream_format: stream_format})
      |> via_in(Pad.ref(:input, :opus),
        options: [
          track_name: track_name,
          timescale: 48_000,
          publisher_priority: 127,
          publisher_max_latency: 45_000,
          retention: :all
        ]
      )
      |> child(:sink, %Sink{
        endpoint: endpoint(),
        protocol: :moq_lite_05,
        namespace: namespace,
        connect_options: tls_options(),
        timeout: @timeout,
        track_demand_events: true
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace}, @timeout)
    assert_pipeline_notified(pipeline, :sink, {:track_ready, _, ^track_name}, @timeout)

    {first, first_subscription} =
      connect_when_routable(pipeline, track_name, track_ref, @timeout)

    second = connect_subscriber()
    {:ok, second_subscription} = MOQX.subscribe(second, track_ref)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_demand, :output, %{active?: true}},
      @timeout
    )

    refute_pipeline_notified(pipeline, :source, {:track_demand, :output, %{active?: true}}, 250)
    refute_pipeline_notified(pipeline, :sink, {:subscriber_joined, ^track_name, _, 2}, 250)

    publish_group(pipeline, 0, 0, "both")
    assert_group(first, first_subscription, 0, "both")
    assert_group(second, second_subscription, 0, "both")

    assert :ok = MOQX.unsubscribe(first, first_subscription)
    refute_pipeline_notified(pipeline, :sink, {:subscriber_left, ^track_name, _, _}, 250)
    refute_pipeline_notified(pipeline, :source, {:track_demand, :output, %{active?: false}}, 250)

    publish_group(pipeline, 1, 960, "remaining")
    assert_object(second, second_subscription, 1, "remaining")

    assert :ok = MOQX.unsubscribe(second, second_subscription)
    assert_pipeline_notified(pipeline, :sink, {:subscriber_left, ^track_name, _, 0}, @timeout)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_demand, :output, %{active?: false}},
      @timeout
    )

    third = connect_subscriber()

    {:ok, third_subscription} =
      MOQX.subscribe(third, track_ref,
        filter: %MOQX.SubscriptionFilter{type: :absolute_start, start_location: {2, 0}}
      )

    assert_pipeline_notified(pipeline, :sink, {:subscriber_joined, ^track_name, _, 1}, @timeout)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_demand, :output, %{active?: true}},
      @timeout
    )

    publish_group(pipeline, 2, 1_920, "resubscribed")
    assert_group(third, third_subscription, 2, "resubscribed")

    abrupt_started = System.monotonic_time(:millisecond)
    assert :ok = MOQX.close(third)
    assert_pipeline_notified(pipeline, :sink, {:subscriber_left, ^track_name, _, 0}, @timeout)
    assert System.monotonic_time(:millisecond) - abrupt_started < @timeout

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_demand, :output, %{active?: false}},
      @timeout
    )

    refute_pipeline_notified(pipeline, :source, {:track_demand, :output, %{active?: false}}, 250)

    assert :ok = MOQX.close(first)
    assert :ok = MOQX.close(second)
    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  defp publish_group(pipeline, _expected_group_id, pts_ticks, payload) do
    buffer = %Buffer{
      payload: payload,
      pts: div(pts_ticks * 1_000_000_000, 48_000),
      metadata: %{moqx: %Unit{group_end?: true}}
    }

    assert :ok = Testing.Pipeline.notify_child(pipeline, :source, {:publish, [buffer]})
  end

  defp assert_group(client, subscription, group_id, payload) do
    assert_receive {:moqx, ^client,
                    %MOQX.Event.SubscriptionAccepted{subscription: ^subscription}},
                   @timeout

    assert_object(client, subscription, group_id, payload)
  end

  defp assert_object(client, subscription, group_id, payload) do
    assert_receive {:moqx, ^client,
                    %MOQX.Event.ObjectReceived{
                      object: %MOQX.Object{
                        subscription: ^subscription,
                        group_id: ^group_id,
                        payload: ^payload
                      }
                    }},
                   @timeout
  end

  defp connect_subscriber do
    assert {:ok, client} =
             MOQX.connect(endpoint(),
               protocol: :moq_lite_05,
               role: :subscriber,
               connect_options: tls_options(),
               timeout: @timeout
             )

    client
  end

  defp connect_when_routable(pipeline, track_name, track_ref, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    try_subscription(pipeline, track_name, track_ref, deadline)
  end

  defp try_subscription(pipeline, track_name, track_ref, deadline) do
    client = connect_subscriber()
    assert {:ok, subscription} = MOQX.subscribe(client, track_ref)
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {Testing.Pipeline, ^pipeline,
       {:handle_child_notification, {{:subscriber_joined, ^track_name, _, 1}, :sink}}} ->
        {client, subscription}

      {:moqx, ^client, %MOQX.Event.SubscriptionFailed{subscription: ^subscription}} ->
        retry_subscription(client, pipeline, track_name, track_ref, deadline)
    after
      remaining -> retry_subscription(client, pipeline, track_name, track_ref, deadline)
    end
  end

  defp retry_subscription(client, pipeline, track_name, track_ref, deadline) do
    _result = MOQX.close(client)

    if System.monotonic_time(:millisecond) >= deadline do
      flunk("MoQ Lite relay did not route the unique publication")
    else
      try_subscription(pipeline, track_name, track_ref, deadline)
    end
  end

  defp endpoint do
    System.get_env("MOQX_LITE_ENDPOINT", "moql://localhost:4463/")
  end

  defp tls_options do
    case System.get_env("MOQX_LITE_CA_FILE") do
      nil -> []
      path -> [cacertfile: path]
    end
  end
end
