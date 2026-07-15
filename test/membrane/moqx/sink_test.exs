defmodule Membrane.MOQX.TestPacketFormat do
  @moduledoc false
  defstruct [:codec]
end

defmodule Membrane.MOQX.TestPacketAdapter do
  @moduledoc false
  @behaviour Membrane.MOQX.TrackAdapter

  @impl true
  def describe(%Membrane.MOQX.TestPacketFormat{codec: codec}, _options) do
    {:ok,
     %Membrane.MOQX.TrackDescriptor{
       packaging: :packet,
       content_types: [:audio],
       initialization: nil,
       codecs: [codec]
     }}
  end

  @impl true
  def publication_unit(%Membrane.Buffer{payload: payload}, _descriptor) do
    {:ok, %Membrane.MOQX.PublicationUnit{payload: payload, segment_end?: true}}
  end
end

defmodule Membrane.MOQX.SinkTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}
  alias Membrane.CMAF.Track
  alias Membrane.MOQX.{Sink, TestDynamicSource, TestRelay}
  alias Membrane.Testing

  require Pad

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

    assert_pipeline_notified(pipeline, :sink, {:subscriber_joined, "video.m4s", 5})
    assert_pipeline_notified(pipeline, :sink, {:subscriber_left, "video.m4s", 5})

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
      |> via_in(Pad.ref(:input, :audio),
        options: [
          track_name: "audio.m4s",
          init_track_name: "audio.init.mp4",
          retention: :latest,
          language: "en",
          render_group: 1,
          alt_group: 2
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
      |> via_in(Pad.ref(:input, :custom),
        options: [
          track_name: "audio.packet",
          adapter: Membrane.MOQX.TestPacketAdapter,
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
