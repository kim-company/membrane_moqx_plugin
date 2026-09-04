defmodule Membrane.MOQX.SourceTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.Buffer
  alias Membrane.MOQX.{Session, Source, TestLite05Publisher, TestPublisher, Track, Unit}
  alias Membrane.Testing

  test "subscribes to one track and emits canonical buffers with received coordinates" do
    track_ref = %MOQX.TrackRef{namespace: ["live", "source"], track: "captions"}

    stream_format = %Track{
      packaging: "webvtt",
      initialization: nil,
      selection_params: %{"mimeType" => "text/vtt"}
    }

    objects = [
      %MOQX.Object{
        group_id: 7,
        subgroup_id: 0,
        object_id: 0,
        publisher_priority: 42,
        payload: "first"
      },
      %MOQX.Object{
        group_id: 7,
        subgroup_id: 0,
        object_id: 1,
        publisher_priority: 42,
        payload: "middle"
      },
      %MOQX.Object{
        group_id: 8,
        subgroup_id: 0,
        object_id: 0,
        publisher_priority: 41,
        payload: "second"
      }
    ]

    publisher = TestPublisher.start(track_ref, objects)

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    spec =
      child(:source, %Source{
        endpoint: publisher.endpoint,
        protocol: :cloudflare_draft_14,
        track: track_ref,
        stream_format: stream_format,
        transport: TestPublisher.transport(publisher),
        subscription_options: [delivery_timeout: 50]
      })
      |> child(:sink, %Testing.Sink{})

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_stream_format(pipeline, :sink, ^stream_format)

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "first",
        metadata: %{
          moqx: %Unit{
            group_end?: false,
            group_id: 7,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 42,
            status: nil
          }
        }
      }
    )

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "middle",
        metadata: %{
          moqx: %Unit{
            group_end?: true,
            group_id: 7,
            subgroup_id: 0,
            object_id: 1,
            publisher_priority: 42,
            status: nil
          }
        }
      }
    )

    assert_receive {Testing.Pipeline, ^pipeline,
                    {:handle_child_notification,
                     {{:buffer, %Buffer{payload: "second"} = second_buffer}, :sink}}},
                   2_000

    assert second_buffer.metadata == %{
             moqx: %Unit{
               group_end?: true,
               group_id: 8,
               subgroup_id: 0,
               object_id: 0,
               publisher_priority: 41,
               status: nil
             }
           }

    assert_end_of_stream(pipeline, :sink)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end

  test "can share a session owned outside the one-track Source" do
    track_ref = %MOQX.TrackRef{namespace: ["live", "shared"], track: "metadata"}
    stream_format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      TestPublisher.start(track_ref, [
        %MOQX.Object{group_id: 3, subgroup_id: 0, object_id: 0, payload: "shared"}
      ])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    {:ok, session} =
      Session.start_link(
        endpoint: publisher.endpoint,
        protocol: :cloudflare_draft_14,
        transport: TestPublisher.transport(publisher)
      )

    spec =
      child(:source, %Source{
        session: session,
        track: track_ref,
        stream_format: stream_format,
        subscription_options: [delivery_timeout: 50]
      })
      |> child(:sink, %Testing.Sink{})

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_stream_format(pipeline, :sink, ^stream_format)

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "shared",
        metadata: %{
          moqx: %Unit{group_end?: true, group_id: 3, subgroup_id: 0, object_id: 0}
        }
      }
    )

    assert_end_of_stream(pipeline, :sink)
    assert Process.alive?(session)
    assert :ok = Session.close(session)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end

  test "matches subgroup completion by group and subgroup identity" do
    track_ref = %MOQX.TrackRef{namespace: ["live", "interleaved"], track: "captions"}
    stream_format = %Track{packaging: "webvtt", initialization: nil}

    publisher =
      TestPublisher.start_interleaved(
        track_ref,
        [
          {%MOQX.Object{group_id: 7, subgroup_id: 0, object_id: 0, payload: "first"},
           %MOQX.Object{group_id: 7, subgroup_id: 0, object_id: 1, payload: "last"}},
          %MOQX.Object{group_id: 7, subgroup_id: 1, object_id: 0, payload: "other"}
        ]
      )

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    spec =
      child(:source, %Source{
        endpoint: publisher.endpoint,
        protocol: :cloudflare_draft_14,
        track: track_ref,
        stream_format: stream_format,
        transport: TestPublisher.transport(publisher),
        subscription_options: [delivery_timeout: 500]
      })
      |> child(:sink, %Testing.Sink{})

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "first"})
    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "other"})

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "last",
        metadata: %{
          moqx: %Unit{
            group_end?: true,
            group_id: 7,
            subgroup_id: 0,
            object_id: 1
          }
        }
      }
    )

    assert_end_of_stream(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end

  test "uses the MoQ Lite track timescale and frame timestamps for buffer PTS" do
    track_ref = %MOQX.TrackRef{namespace: ["live"], track: "opus"}
    stream_format = %Track{packaging: "opus", initialization: nil}

    publisher = TestLite05Publisher.start(track_ref, 48_000, [{48_000, "first"}, {960, "second"}])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    spec =
      child(:source, %Source{
        endpoint: publisher.endpoint,
        protocol: :moq_lite_05,
        track: track_ref,
        stream_format: stream_format,
        transport: TestLite05Publisher.transport(publisher)
      })
      |> child(:sink, %Testing.Sink{})

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "first",
        pts: 1_000_000_000,
        metadata: %{moqx: %Unit{publisher_priority: 17}}
      }
    )

    TestLite05Publisher.finish_group(publisher)

    assert_receive {Testing.Pipeline, ^pipeline,
                    {:handle_child_notification,
                     {{:buffer, %Buffer{payload: "second"} = second}, :sink}}}

    assert second.pts == 1_020_000_000
    TestLite05Publisher.finish_subscription(publisher)
    assert_end_of_stream(pipeline, :sink)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end
end
