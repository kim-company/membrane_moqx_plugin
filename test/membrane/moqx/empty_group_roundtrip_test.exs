defmodule Membrane.MOQX.EmptyGroupRoundtripTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}
  alias Membrane.MOQX.Event.EmptyGroup
  alias Membrane.MOQX.{Sink, Source, TestControlledSource, TestLiteBridge, Track, Unit}
  require Pad

  test "an empty group separates backward timestamp epochs without becoming EOS" do
    relay = TestLiteBridge.start()
    namespace = ["epochs", "raw"]
    format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :media), options: [track_name: "media", timescale: 1_000_000])
          |> child(:publisher, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :publisher, {:track_ready, _, "media"})

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
          |> child(:consumer, Testing.Sink)
      )

    assert_pipeline_notified(publisher, :publisher, {:subscriber_joined, "media", _, 1})

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish,
       [
         %Buffer{payload: "before", pts: 5_000_000, metadata: %{moqx: %Unit{group_end?: true}}}
       ]}
    )

    assert_sink_buffer(subscriber, :consumer, %Buffer{payload: "before", pts: 5_000_000})
    Testing.Pipeline.notify_child(publisher, :producer, {:event, %EmptyGroup{group_id: 999}})
    assert_sink_event(subscriber, :consumer, %EmptyGroup{group_id: 1})

    refute_receive {Testing.Pipeline, ^subscriber,
                    {:handle_element_end_of_stream, {:consumer, :input}}},
                   20

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:publish,
       [
         %Buffer{payload: "after", pts: 1_000_000, metadata: %{moqx: %Unit{group_end?: true}}}
       ]}
    )

    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    assert_sink_buffer(subscriber, :consumer, %Buffer{
      payload: "after",
      pts: 1_000_000,
      metadata: %{moqx: %Unit{group_id: 2}}
    })

    assert_end_of_stream(subscriber, :consumer, :input)
    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end
end
