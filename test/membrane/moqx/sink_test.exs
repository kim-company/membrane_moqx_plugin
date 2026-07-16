defmodule Membrane.MOQX.TestPacketFormat do
  @moduledoc false
  defstruct [:codec]
end

defmodule Membrane.MOQX.TestPacketAdapter do
  @moduledoc false
  @behaviour Membrane.MOQX.TrackAdapter

  @impl true
  def to_moqx_stream_format(%Membrane.MOQX.TestPacketFormat{codec: codec}, _options) do
    track =
      %Membrane.MOQX.Track{
        packaging: "packet",
        initialization: nil,
        selection_params: %{"codec" => codec}
      }

    {:ok, track, track}
  end

  @impl true
  def to_moqx_buffer(%Membrane.Buffer{} = buffer, %Membrane.MOQX.Track{} = track) do
    buffer = %{buffer | metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}}
    {:ok, buffer, track}
  end
end

defmodule Membrane.MOQX.SinkTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}
  alias Membrane.CMAF.Track
  alias Membrane.MOQX.{Sink, TestControlledSource, TestDynamicSource, TestRelay}
  alias Membrane.MOQX.TrackAdapter.ToTrack
  alias Membrane.Testing

  require Pad

  test "surfaces and rejects a controlled subscription for an unknown track" do
    namespace = ["live", "pull"]
    relay = TestRelay.start(namespace)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: relay.endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            transport: TestRelay.transport(relay),
            inbound_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    subscriber = Task.async(fn -> TestRelay.subscribe(relay, "subtitles/it") end)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested,
       %MOQX.PublicationSubscriptionRequest{
         track: %MOQX.TrackRef{namespace: ^namespace, track: "subtitles/it"},
         filter: %MOQX.SubscriptionFilter{type: :largest_object}
       } = request}
    )

    rejection = %MOQX.SubscriptionRejection{code: :unauthorized, reason: "not allowed"}

    assert :ok =
             Testing.Pipeline.notify_child(
               pipeline,
               :sink,
               {:reject_subscription, request, rejection}
             )

    assert {:error, %{code: 1, reason: "not allowed"}} = Task.await(subscriber)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "accepts and tracks a controlled subscription for a ready track" do
    namespace = ["live", "controlled-ready"]
    relay = TestRelay.start(namespace)

    stream_format = %Membrane.MOQX.Track{packaging: "webvtt", initialization: nil}

    buffer = %Buffer{
      payload: "WEBVTT\n\nReady",
      metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
    }

    spec =
      child(:source, %TestControlledSource{stream_format: stream_format})
      |> via_in(Pad.ref(:input, :subtitles),
        options: [track_name: "subtitles/ready", retention: :live]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay),
        inbound_subscriptions: :controlled
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :subtitles), "subtitles/ready"}
    )

    subscriber = Task.async(fn -> TestRelay.subscribe(relay, "subtitles/ready") end)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request}
    )

    assert :ok =
             Testing.Pipeline.notify_child(pipeline, :sink, {:accept_subscription, request})

    assert {:ok, request_id} = Task.await(subscriber)
    request_handle = request.handle

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscriber_joined, "subtitles/ready", ^request_handle, 1}
    )

    assert :ok = Testing.Pipeline.notify_child(pipeline, :source, {:publish, [buffer]})
    assert {:ok, [object]} = TestRelay.receive_subscription_objects(relay, 1)
    assert object.payload == buffer.payload

    assert :ok = TestRelay.unsubscribe(relay, request_id)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscriber_left, "subtitles/ready", ^request_handle, 0}
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "provisions an approved unknown track when its dynamic pad becomes ready" do
    namespace = ["live", "pull-provision"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay),
      inbound_subscriptions: :controlled
    }

    pipeline = Testing.Pipeline.start_link_supervised!(spec: child(:sink, sink))
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    subscriber = Task.async(fn -> TestRelay.subscribe(relay, "subtitles/on-demand") end)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request}
    )

    assert :ok =
             Testing.Pipeline.notify_child(pipeline, :sink, {:accept_subscription, request})

    stream_format = %Membrane.MOQX.Track{packaging: "webvtt", initialization: nil}

    buffer = %Buffer{
      payload: "WEBVTT\n\nOn demand",
      metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
    }

    track_spec =
      child(:source, %TestDynamicSource{stream_format: stream_format, buffer: buffer})
      |> via_out(Pad.ref(:output, :subtitles))
      |> via_in(Pad.ref(:input, :subtitles),
        options: [track_name: "subtitles/on-demand", retention: :latest]
      )
      |> get_child(:sink)

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: track_spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :subtitles), "subtitles/on-demand"}
    )

    assert {:ok, request_id} = Task.await(subscriber)
    request_handle = request.handle

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscriber_joined, "subtitles/on-demand", ^request_handle, 1}
    )

    assert {:ok, [object]} = TestRelay.receive_subscription_objects(relay, 1)
    assert object.payload == buffer.payload

    assert :ok = TestRelay.unsubscribe(relay, request_id)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "invalidates a controlled request cancelled while pending" do
    namespace = ["live", "pending-cancel"]
    relay = TestRelay.start(namespace)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: relay.endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            transport: TestRelay.transport(relay),
            inbound_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})
    assert {:ok, request_id} = TestRelay.request_subscription(relay, "future")

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request}
    )

    assert :ok = TestRelay.cancel_pending_subscription(relay, request_id)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_cancelled, ^request, :unsubscribed}
    )

    assert :ok =
             Testing.Pipeline.notify_child(pipeline, :sink, {:accept_subscription, request})

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_decision_failed, ^request, :unknown_subscription_request}
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "surfaces decision timeout and invalidates the pending request" do
    namespace = ["live", "decision-timeout"]
    relay = TestRelay.start(namespace)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: relay.endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            transport: TestRelay.transport(relay),
            inbound_subscriptions: :controlled,
            subscription_decision_timeout: 25
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})
    assert {:ok, request_id} = TestRelay.request_subscription(relay, "future")

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request}
    )

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_cancelled, ^request, :decision_timeout}
    )

    assert {:error, %{code: 2, reason: "subscription decision timed out"}} =
             TestRelay.await_subscription_result(relay, request_id)

    assert :ok =
             Testing.Pipeline.notify_child(pipeline, :sink, {:accept_subscription, request})

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_decision_failed, ^request, :unknown_subscription_request}
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "emits aggregate track demand events on subscriber boundary transitions" do
    namespace = ["live", "track-demand"]
    relay = TestRelay.start(namespace)

    stream_format = %Membrane.MOQX.Track{packaging: "webvtt", initialization: nil}

    buffer = %Buffer{
      payload: "WEBVTT\n\nDemand",
      metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
    }

    spec =
      child(:source, %TestDynamicSource{stream_format: stream_format, buffer: buffer})
      |> via_out(Pad.ref(:output, :subtitles))
      |> via_in(Pad.ref(:input, :subtitles),
        options: [track_name: "subtitles/demand", retention: :latest]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay),
        track_demand_events: true
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :subtitles), "subtitles/demand"}
    )

    assert {:ok, %{"subtitles/demand" => _object}} =
             TestRelay.capture(relay, ["subtitles/demand"])

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_demand, Pad.ref(:output, :subtitles),
       %{
         __struct__: Membrane.MOQX.Event.TrackDemand,
         subscriber_count: 1,
         active?: true
       }}
    )

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_demand, Pad.ref(:output, :subtitles),
       %{
         __struct__: Membrane.MOQX.Event.TrackDemand,
         subscriber_count: 0,
         active?: false
       }}
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "applies explicit infrastructure subscription policy" do
    namespace = ["live", "infrastructure"]
    relay = TestRelay.start(namespace)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: relay.endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            transport: TestRelay.transport(relay),
            inbound_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})
    assert {:ok, request_id} = TestRelay.request_subscription(relay, ".catalog")
    assert {:ok, ^request_id} = TestRelay.await_subscription_result(relay, request_id)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscriber_joined, ".catalog", _identity, 1}
    )

    refute_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{}},
      100
    )

    assert :ok = TestRelay.unsubscribe(relay, request_id)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)

    controlled_relay = TestRelay.start(namespace)

    controlled_pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: controlled_relay.endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            transport: TestRelay.transport(controlled_relay),
            inbound_subscriptions: :controlled,
            infrastructure_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(controlled_pipeline, :sink, {:publication_ready, ^namespace})

    assert {:ok, controlled_request_id} =
             TestRelay.request_subscription(controlled_relay, ".catalog")

    assert_pipeline_notified(
      controlled_pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request}
    )

    rejection = %MOQX.SubscriptionRejection{code: :unauthorized}

    assert :ok =
             Testing.Pipeline.notify_child(
               controlled_pipeline,
               :sink,
               {:reject_subscription, request, rejection}
             )

    assert {:error, %{code: 1}} =
             TestRelay.await_subscription_result(controlled_relay, controlled_request_id)

    assert :ok = Testing.Pipeline.terminate(controlled_pipeline)
    assert :ok = TestRelay.await_shutdown(controlled_relay)
  end

  test "coalesces multiple approved requests onto one dynamically provisioned track" do
    namespace = ["live", "coalesced"]
    relay = TestRelay.start(namespace)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: relay.endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            transport: TestRelay.transport(relay),
            inbound_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})
    assert {:ok, first_id} = TestRelay.request_subscription(relay, "shared")
    assert {:ok, second_id} = TestRelay.request_subscription(relay, "shared")

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = first_request}
    )

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = second_request}
    )

    assert :ok =
             Testing.Pipeline.notify_child(
               pipeline,
               :sink,
               {:accept_subscription, first_request}
             )

    assert :ok =
             Testing.Pipeline.notify_child(
               pipeline,
               :sink,
               {:accept_subscription, second_request}
             )

    stream_format = %Membrane.MOQX.Track{packaging: "opaque", initialization: nil}

    buffer = %Buffer{
      payload: "one producer",
      metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
    }

    track_spec =
      child(:source, %TestDynamicSource{stream_format: stream_format, buffer: buffer})
      |> via_out(Pad.ref(:output, :shared))
      |> via_in(Pad.ref(:input, :shared), options: [track_name: "shared", retention: :latest])
      |> get_child(:sink)

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: track_spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :shared), "shared"}
    )

    assert {:ok, results} = TestRelay.await_subscription_results(relay, [first_id, second_id])
    assert results == %{first_id => {:ok, first_id}, second_id => {:ok, second_id}}

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscriber_joined, "shared", first_identity, 1}
    )

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:subscriber_joined, "shared", second_identity, 2}
    )

    assert MapSet.new([first_identity, second_identity]) ==
             MapSet.new([first_request.handle, second_request.handle])

    assert {:ok, objects} = TestRelay.receive_subscription_objects(relay, 2)
    assert Enum.map(objects, & &1.payload) == [buffer.payload, buffer.payload]

    assert :ok = TestRelay.unsubscribe(relay, first_id)
    assert_pipeline_notified(pipeline, :sink, {:subscriber_left, "shared", _identity, 1})

    assert :ok = TestRelay.unsubscribe(relay, second_id)
    assert_pipeline_notified(pipeline, :sink, {:subscriber_left, "shared", _identity, 0})

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "publishes a generic subtitle track without audio or video assumptions" do
    namespace = ["live", "subtitles"]
    relay = TestRelay.start(namespace)

    stream_format = %Membrane.MOQX.Track{
      packaging: "webvtt",
      initialization: nil,
      selection_params: %{"mimeType" => "text/vtt", "lang" => "it"},
      catalog_fields: %{"label" => "Italian subtitles"}
    }

    buffer = %Buffer{
      payload: "WEBVTT\n\n00:00.000 --> 00:02.000\nCiao!",
      metadata: %{moqx: %Membrane.MOQX.Unit{group_end?: true}}
    }

    spec =
      child(:source, %TestDynamicSource{stream_format: stream_format, buffer: buffer})
      |> via_out(Pad.ref(:output, :subtitles))
      |> via_in(Pad.ref(:input, :subtitles),
        options: [track_name: "subtitles.it.vtt", retention: :latest]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay)
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :subtitles), "subtitles.it.vtt"}
    )

    assert {:ok, objects} = TestRelay.capture(relay, [".catalog", "subtitles.it.vtt"])
    assert objects["subtitles.it.vtt"].payload == buffer.payload

    assert %{
             "tracks" => [
               %{
                 "name" => "subtitles.it.vtt",
                 "packaging" => "webvtt",
                 "selectionParams" => %{"mimeType" => "text/vtt", "lang" => "it"},
                 "label" => "Italian subtitles"
               }
             ]
           } = JSON.decode!(objects[".catalog"].payload)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "publishes an unchanged H264 CMAF track through the public Sink" do
    namespace = ["live", "camera-1"]
    relay = TestRelay.start(namespace)

    stream_format = %Track{
      content_type: :video,
      header: "cmaf-init",
      resolution: {1920, 1080},
      codecs: %{
        avc1: %{profile: "42", compatibility: "C0", level: "1F"}
      }
    }

    buffer = %Buffer{
      payload: "unchanged-cmaf-segment",
      metadata: %{duration: 2_000, independent?: true}
    }

    spec =
      child(:source, %TestDynamicSource{stream_format: stream_format, buffer: buffer})
      |> via_out(Pad.ref(:output, :video))
      |> child(:adapter, %ToTrack{adapter: Membrane.MOQX.TrackAdapter.CMAF})
      |> via_out(Pad.ref(:output, :video))
      |> via_in(Pad.ref(:input, :video),
        options: [
          track_name: "video.m4s",
          init_track_name: "video.init.mp4",
          retention: :latest
        ]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay)
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :video), "video.m4s"}
    )

    assert {:ok, objects} =
             TestRelay.capture(relay, [".catalog", "video.init.mp4", "video.m4s"])

    catalog = objects[".catalog"]
    init = objects["video.init.mp4"]
    media = objects["video.m4s"]

    assert %{
             "supportsDeltaUpdates" => false,
             "commonTrackFields" => %{
               "namespace" => "live/camera-1"
             },
             "tracks" => [
               %{
                 "name" => "video.m4s",
                 "initTrack" => "video.init.mp4",
                 "packaging" => "cmaf",
                 "selectionParams" => %{
                   "codec" => "avc1.42C01F",
                   "height" => 1080,
                   "width" => 1920
                 }
               }
             ]
           } = JSON.decode!(catalog.payload)

    assert {init.group_id, init.object_id, init.payload} == {0, 0, "cmaf-init"}

    assert {media.group_id, media.object_id, media.payload} ==
             {0, 0, "unchanged-cmaf-segment"}

    assert_pipeline_notified(pipeline, :sink, {:subscriber_joined, "video.m4s", 5, 1})
    assert_pipeline_notified(pipeline, :sink, {:subscriber_left, "video.m4s", 5, 0})

    :ok =
      Testing.Pipeline.execute_actions(
        pipeline,
        remove_link: {:sink, Pad.ref(:input, :video)}
      )

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_removed, Pad.ref(:input, :video), "video.m4s"}
    )

    assert {:ok, [end_object]} = TestRelay.capture_many(relay, "video.m4s", 1)

    assert {end_object.group_id, end_object.object_id, end_object.status, end_object.payload} ==
             {1, 0, :end_of_track, <<>>}

    assert {:ok, %{".catalog" => removed_catalog}} = TestRelay.capture(relay, [".catalog"])
    assert %{"tracks" => []} = JSON.decode!(removed_catalog.payload)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "maps CMAF chunks and segments to deterministic MOQ coordinates" do
    namespace = ["live", "coordinates"]
    relay = TestRelay.start(namespace)

    stream_format = %Track{
      content_type: :video,
      header: "init",
      codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "1F"}}
    }

    buffers = [
      %Buffer{payload: "chunk-1", metadata: %{last_chunk?: false}},
      %Buffer{payload: "chunk-2", metadata: %{last_chunk?: true}},
      %Buffer{payload: "segment-2", metadata: %{}}
    ]

    spec =
      child(:source, %Testing.Source{
        output: Testing.Source.output_from_buffers(buffers),
        stream_format: stream_format
      })
      |> child(:adapter, %ToTrack{adapter: Membrane.MOQX.TrackAdapter.CMAF})
      |> via_out(Pad.ref(:output, :video))
      |> via_in(Pad.ref(:input, :video),
        options: [track_name: "video.m4s", retention: :all]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay)
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :video), "video.m4s"}
    )

    assert {:ok, objects} = TestRelay.capture_many(relay, "video.m4s", 4)

    assert Enum.map(objects, &{&1.group_id, &1.object_id, &1.status, &1.payload}) == [
             {0, 0, nil, "chunk-1"},
             {0, 1, nil, "chunk-2"},
             {1, 0, nil, "segment-2"},
             {2, 0, :end_of_track, <<>>}
           ]

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ended, Pad.ref(:input, :video), "video.m4s"}
    )

    assert {:ok, %{".catalog" => catalog}} = TestRelay.capture(relay, [".catalog"])
    assert %{"tracks" => []} = JSON.decode!(catalog.payload)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "publishes a new initialization generation before advertising a format change" do
    namespace = ["live", "format-change"]
    relay = TestRelay.start(namespace)

    initial_format = %Track{
      content_type: :video,
      header: "init-v1",
      resolution: {1280, 720},
      codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "1F"}}
    }

    updated_format = %Track{
      content_type: :video,
      header: "init-v2",
      resolution: {1920, 1080},
      codecs: %{avc1: %{profile: "64", compatibility: "00", level: "28"}}
    }

    generator = fn
      :ready, _size ->
        {[
           buffer: {:output, %Buffer{payload: "v1"}},
           stream_format: {:output, updated_format},
           buffer: {:output, %Buffer{payload: "v2"}}
         ], :sent}

      :sent, _size ->
        {[], :sent}
    end

    spec =
      child(:source, %Testing.Source{
        output: {:ready, generator},
        stream_format: initial_format
      })
      |> child(:adapter, %ToTrack{adapter: Membrane.MOQX.TrackAdapter.CMAF})
      |> via_out(Pad.ref(:output, :video))
      |> via_in(Pad.ref(:input, :video),
        options: [
          track_name: "video.m4s",
          init_track_name: "video.init.mp4",
          retention: :latest
        ]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay)
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_updated, Pad.ref(:input, :video), "video.m4s", 1}
    )

    assert {:ok, objects} =
             TestRelay.capture(relay, [".catalog", "video.init.mp4.1", "video.m4s"])

    assert objects["video.init.mp4.1"].payload == "init-v2"
    assert objects["video.m4s"].payload == "v2"

    assert %{
             "tracks" => [
               %{
                 "initTrack" => "video.init.mp4.1",
                 "selectionParams" => %{
                   "codec" => "avc1.640028",
                   "height" => 1080,
                   "width" => 1920
                 }
               }
             ]
           } = JSON.decode!(objects[".catalog"].payload)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "accepts a dynamic pad added after the publication is already playing" do
    namespace = ["live", "late-pad"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    pipeline = Testing.Pipeline.start_link_supervised!(spec: child(:sink, sink))
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    format = %Track{
      content_type: :audio,
      header: "aac-init",
      codecs: %{mp4a: %{aot_id: "2", channels: 2, frequency: 48_000}}
    }

    generator = fn
      :ready, _size -> {[buffer: {:output, %Buffer{payload: "aac-cmaf"}}], :sent}
      :sent, _size -> {[], :sent}
    end

    late_pad_spec =
      child(:late_source, %Testing.Source{output: {:ready, generator}, stream_format: format})
      |> child(:late_adapter, %ToTrack{
        adapter: Membrane.MOQX.TrackAdapter.CMAF,
        adapter_options: [
          selection_params: %{"lang" => "en"},
          catalog_fields: %{"renderGroup" => 1, "altGroup" => 2}
        ]
      })
      |> via_out(Pad.ref(:output, :audio))
      |> via_in(Pad.ref(:input, :audio),
        options: [
          track_name: "audio.m4s",
          init_track_name: "audio.init.mp4",
          retention: :latest
        ]
      )
      |> get_child(:sink)

    :ok = Testing.Pipeline.execute_actions(pipeline, spec: late_pad_spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :audio), "audio.m4s"}
    )

    assert {:ok, objects} =
             TestRelay.capture(relay, [".catalog", "audio.init.mp4", "audio.m4s"])

    assert objects["audio.init.mp4"].payload == "aac-init"
    assert objects["audio.m4s"].payload == "aac-cmaf"

    assert %{
             "tracks" => [
               %{
                 "renderGroup" => 1,
                 "altGroup" => 2,
                 "selectionParams" => %{
                   "codec" => "mp4a.40.2",
                   "samplerate" => 48_000,
                   "channelConfig" => 2,
                   "lang" => "en"
                 }
               }
             ]
           } = JSON.decode!(objects[".catalog"].payload)

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "publishes a caller-adapted track without an initialization object" do
    namespace = ["live", "custom-adapter"]
    relay = TestRelay.start(namespace)

    spec =
      child(:source, %TestDynamicSource{
        stream_format: %Membrane.MOQX.TestPacketFormat{codec: "packet.audio"},
        buffer: %Buffer{payload: "custom-packet"}
      })
      |> via_out(Pad.ref(:output, :custom))
      |> child(:custom_adapter, %ToTrack{adapter: Membrane.MOQX.TestPacketAdapter})
      |> via_out(Pad.ref(:output, :custom))
      |> via_in(Pad.ref(:input, :custom),
        options: [
          track_name: "audio.packet",
          retention: :latest
        ]
      )
      |> child(:sink, %Sink{
        endpoint: relay.endpoint,
        protocol: :cloudflare_draft_14,
        namespace: namespace,
        transport: TestRelay.transport(relay)
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :custom), "audio.packet"}
    )

    assert {:ok, objects} = TestRelay.capture(relay, [".catalog", "audio.packet"])
    assert objects["audio.packet"].payload == "custom-packet"

    assert %{
             "commonTrackFields" => %{"namespace" => "live/custom-adapter"},
             "tracks" => [
               %{
                 "name" => "audio.packet",
                 "packaging" => "packet",
                 "selectionParams" => %{"codec" => "packet.audio"}
               }
             ]
           } = JSON.decode!(objects[".catalog"].payload)

    refute JSON.decode!(objects[".catalog"].payload)["tracks"]
           |> hd()
           |> Map.has_key?("initTrack")

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "reports relay cancellation and releases the Sink" do
    namespace = ["live", "relay-cancel"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    spec = {child(:sink, sink), group: :fatal_sink, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    assert :ok = TestRelay.cancel_publication(relay, 2, "withdrawn")

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:publication_cancelled,
       %MOQX.ProtocolError{
         protocol: :cloudflare_draft_14,
         operation: :publish,
         code: 2,
         reason: "withdrawn"
       }}
    )

    assert_child_terminated(pipeline, :sink)

    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "closes the MOQX connection when the Sink is killed" do
    namespace = ["live", "sink-killed"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    spec = {child(:sink, sink), group: :fatal_sink, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    sink_pid = Testing.Pipeline.get_child_pid!(pipeline, :sink)
    sink_monitor = Process.monitor(sink_pid)
    Process.exit(sink_pid, :kill)

    assert_receive {:DOWN, ^sink_monitor, :process, ^sink_pid, :killed}
    assert :ok = TestRelay.await_connection_close(relay)

    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "rejects a dynamic pad whose track name is already owned" do
    namespace = ["live", "duplicate-track"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    sink_spec = {child(:sink, sink), group: :fatal_sink, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: sink_spec)
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    format = %Track{
      content_type: :audio,
      header: "aac-init",
      codecs: %{mp4a: %{aot_id: "2", channels: 2, frequency: 48_000}}
    }

    source = fn name, pad_id ->
      child(name, %TestDynamicSource{
        stream_format: format,
        buffer: %Buffer{payload: "aac-cmaf"}
      })
      |> via_out(Pad.ref(:output, pad_id))
      |> child({:adapter, pad_id}, %ToTrack{adapter: Membrane.MOQX.TrackAdapter.CMAF})
      |> via_out(Pad.ref(:output, pad_id))
      |> via_in(Pad.ref(:input, pad_id), options: [track_name: "audio.m4s"])
      |> get_child(:sink)
    end

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: source.(:first_source, :first))

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :first), "audio.m4s"}
    )

    assert :ok =
             Testing.Pipeline.execute_actions(pipeline, spec: source.(:second_source, :second))

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_rejected, Pad.ref(:input, :second), {:duplicate_track_name, "audio.m4s"}}
    )

    assert_child_terminated(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "reports publication rejection and releases the Sink" do
    namespace = ["live", "relay-reject"]
    relay = TestRelay.start(namespace, publication: {:reject, 1, "unauthorized"})

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    spec = {child(:sink, sink), group: :fatal_sink, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:publication_failed,
       %MOQX.ProtocolError{
         protocol: :cloudflare_draft_14,
         operation: :publish,
         code: 1,
         reason: "unauthorized"
       }}
    )

    assert_child_terminated(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestRelay.await_shutdown(relay)
  end

  test "reports an unexpected relay connection close and releases the Sink" do
    namespace = ["live", "relay-close"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    spec = {child(:sink, sink), group: :fatal_sink, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    assert :ok = TestRelay.close_connection(relay, 77)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:connection_closed, %{error_code: 77, initiator: :peer}}
    )

    assert_child_terminated(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "reports a typed protocol failure and releases the Sink" do
    namespace = ["live", "protocol-failure"]
    relay = TestRelay.start(namespace)

    sink = %Sink{
      endpoint: relay.endpoint,
      protocol: :cloudflare_draft_14,
      namespace: namespace,
      transport: TestRelay.transport(relay)
    }

    spec = {child(:sink, sink), group: :fatal_sink, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace})

    assert :ok = TestRelay.fail_protocol(relay)
    assert_pipeline_notified(pipeline, :sink, {:protocol_failed, :invalid_publish_namespace_ok})
    assert_child_terminated(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
  end
end
