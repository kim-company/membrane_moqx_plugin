defmodule Membrane.MOQX.EmptyGroupSourceTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.Buffer
  alias Membrane.MOQX.{Source, TestLite05Publisher, Track, Unit}
  alias Membrane.Testing

  test "a zero-byte object is a timestamped buffer, never an empty-group event" do
    {pipeline, publisher} = start_source([[{10, <<>>}]], :current)
    TestLite05Publisher.finish_group(publisher)

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<>>,
      pts: 10_000_000,
      metadata: %{moqx: %Unit{group_end?: true}}
    })

    finish(pipeline, publisher)
    refute_sink_event(pipeline, :sink, %Membrane.MOQX.Event.EmptyGroup{}, 20)
  end

  test "resetting a zero-object group does not emit a codec-epoch boundary" do
    {pipeline, publisher} = start_source([[]], :current)
    assert_sink_stream_format(pipeline, :sink, %Track{})
    TestLite05Publisher.reset_group(publisher)
    TestLite05Publisher.finish_subscription(publisher)
    # RESET may discard the GROUP header before its ID is received. Do not
    # require EOS for an unseen final group; shutdown is explicit in this case.
    refute_sink_event(pipeline, :sink, %Membrane.MOQX.Event.EmptyGroup{}, 20)
    refute_sink_buffer(pipeline, :sink, _buffer, 20)
    Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "next_group skips an initial empty group, not the following media group" do
    {pipeline, publisher} = start_source([[], [{10, "next"}]], :next_group)
    TestLite05Publisher.finish_group(publisher)
    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "next", pts: 10_000_000})
    refute_sink_event(pipeline, :sink, %Membrane.MOQX.Event.EmptyGroup{}, 20)
    finish(pipeline, publisher)
  end

  defp start_source(groups, start_policy) do
    track = %MOQX.TrackRef{namespace: ["empty-group-policy"], track: "raw"}
    publisher = TestLite05Publisher.start(track, 1000, groups)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            track: track,
            start_policy: start_policy,
            transport: TestLite05Publisher.transport(publisher),
            stream_format: %Track{packaging: "application/example", initialization: nil}
          })
          |> child(:sink, Testing.Sink)
      )

    {pipeline, publisher}
  end

  defp finish(pipeline, publisher) do
    TestLite05Publisher.finish_subscription(publisher)
    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "a complete empty Lite group emits an event between unchanged forward and backward media" do
    track = %MOQX.TrackRef{namespace: ["empty-group"], track: "raw"}
    publisher = TestLite05Publisher.start(track, 1000, [[{100, "before"}], [], [{10, "after"}]])

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            track: track,
            transport: TestLite05Publisher.transport(publisher),
            stream_format: %Track{packaging: "application/example", initialization: nil}
          })
          |> child(:sink, Testing.Sink)
      )

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: "before",
      pts: 100_000_000,
      metadata: %{moqx: %Unit{group_id: 7, object_id: 0, group_end?: true}}
    })

    assert_sink_event(pipeline, :sink, %{__struct__: Membrane.MOQX.Event.EmptyGroup, group_id: 8})
    TestLite05Publisher.finish_group(publisher)

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: "after",
      pts: 10_000_000,
      metadata: %{moqx: %Unit{group_id: 9, object_id: 0, group_end?: true}}
    })

    TestLite05Publisher.finish_subscription(publisher)
    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end
end
