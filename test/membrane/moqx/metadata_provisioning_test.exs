defmodule Membrane.MOQX.MetadataProvisioningTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}
  alias Membrane.MOQX.{Sink, Source, TestControlledSource, TestLiteBridge, Track, Unit}
  require Pad

  for {decision, outcome} <- [reject: :rejected, timeout: :timed_out] do
    @decision decision
    @outcome outcome
    test "missing track metadata #{@decision} is scoped and reports its terminal outcome" do
      relay = TestLiteBridge.start()
      namespace = ["provisioning", Atom.to_string(@decision)]
      ref = %MOQX.TrackRef{namespace: namespace, track: "missing"}

      publisher =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:publisher, %Sink{
              endpoint: relay.publisher_endpoint,
              protocol: :moq_lite_05,
              namespace: namespace,
              transport: relay.transport,
              missing_track_metadata: :controlled,
              inbound_subscriptions: :controlled,
              track_metadata_timeout: if(@decision == :timeout, do: 50, else: 5_000)
            })
        )

      assert_pipeline_notified(publisher, :publisher, {:publication_ready, ^namespace})

      subscriber =
        Testing.Pipeline.start_link_supervised!(
          spec: {
            child(:source, %Source{
              endpoint: relay.subscriber_endpoint,
              protocol: :moq_lite_05,
              transport: relay.transport,
              track: ref,
              stream_format: %Track{packaging: "application/example", initialization: nil}
            })
            |> child(:consumer, Testing.Sink),
            group: :request, crash_group_mode: :temporary
          }
        )

      assert_pipeline_notified(publisher, :publisher, {:track_metadata_requested, request})
      rejection = %MOQX.SubscriptionRejection{code: :track_does_not_exist}

      if @decision == :reject,
        do:
          Testing.Pipeline.notify_child(
            publisher,
            :publisher,
            {:reject_track_request, request, rejection}
          )

      assert_pipeline_notified(
        publisher,
        :publisher,
        {:track_metadata_request_done, %{request: ^request, reason: @outcome}}
      )

      assert_pipeline_notified(subscriber, :source, {:subscription_failed, ^ref, _})

      Testing.Pipeline.notify_child(
        publisher,
        :publisher,
        {:reject_track_request, request, rejection}
      )

      assert_pipeline_notified(
        publisher,
        :publisher,
        {:track_metadata_decision_failed, ^request, :stale_track_request}
      )

      assert Process.alive?(publisher)
      Testing.Pipeline.terminate(subscriber)
      Testing.Pipeline.terminate(publisher)
      assert :ok = TestLiteBridge.stop(relay)
    end
  end

  test "metadata demand provisions a missing track without authorizing its subscription" do
    relay = TestLiteBridge.start()
    namespace = ["provisioning", "controlled"]
    ref = %MOQX.TrackRef{namespace: namespace, track: "media"}
    format = %Track{packaging: "application/example", initialization: nil}

    options =
      struct!(Sink,
        endpoint: relay.publisher_endpoint,
        protocol: :moq_lite_05,
        namespace: namespace,
        transport: relay.transport,
        missing_track_metadata: :controlled,
        inbound_subscriptions: :controlled
      )

    publisher = Testing.Pipeline.start_link_supervised!(spec: child(:publisher, options))
    assert_pipeline_notified(publisher, :publisher, {:publication_ready, ^namespace})

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            endpoint: relay.subscriber_endpoint,
            protocol: :moq_lite_05,
            transport: relay.transport,
            track: ref,
            stream_format: format
          })
          |> child(:consumer, Testing.Sink)
      )

    assert_pipeline_notified(publisher, :publisher, {:track_metadata_requested, request})
    assert request.track == ref

    Testing.Pipeline.execute_actions(publisher,
      spec:
        child(:producer, %TestControlledSource{stream_format: format})
        |> via_in(Pad.ref(:input, :media), options: [track_name: "media", timescale: 1_000_000])
        |> get_child(:publisher)
    )

    assert_pipeline_notified(
      publisher,
      :publisher,
      {:track_metadata_request_done, %{request: ^request, reason: :registered, error: nil}}
    )

    assert_pipeline_notified(publisher, :publisher, {:subscription_requested, admission})
    refute_pipeline_notified(publisher, :publisher, {:subscriber_joined, "media", _, _}, 50)
    Testing.Pipeline.notify_child(publisher, :publisher, {:accept_subscription, admission})
    assert_pipeline_notified(publisher, :publisher, {:subscriber_joined, "media", _, 1})

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish,
       [
         %Buffer{
           payload: "provisioned",
           pts: 3_000_000,
           metadata: %{moqx: %Unit{group_end?: true}}
         }
       ]}
    )

    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)
    assert_sink_buffer(subscriber, :consumer, %Buffer{payload: "provisioned", pts: 3_000_000})
    assert_end_of_stream(subscriber, :consumer, :input)
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end
end
