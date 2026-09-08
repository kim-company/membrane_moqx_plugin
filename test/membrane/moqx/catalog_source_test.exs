defmodule Membrane.MOQX.CatalogSourceTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}

  alias Membrane.MOQX.{
    CatalogSource,
    TestDraft16Publisher,
    TestPublisher,
    Track,
    TrackOffer,
    Unit
  }

  alias Membrane.Testing

  require Pad

  test "forwards rejected selected media errors before terminating its catalog crash group" do
    alias Membrane.MOQX.{Sink, TestLiteBridge}
    relay = TestLiteBridge.start(reject_track: "blocked")
    namespace = ["room", "media-rejected.hang"]

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:publisher, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :publisher, {:publication_ready, ^namespace})

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          {child(:catalog, %CatalogSource{
             endpoint: relay.subscriber_endpoint,
             protocol: :moq_lite_05,
             profile: :hang,
             namespace: namespace,
             transport: relay.transport
           }), group: :catalog_reader, crash_group_mode: :temporary}
      )

    assert_pipeline_notified(pipeline, :catalog, :catalog_ready)
    ref = %MOQX.TrackRef{namespace: namespace, track: "blocked"}

    # Explicitly described selection is a public CatalogSource operation even
    # when the current catalog does not advertise this address.
    Testing.Pipeline.execute_actions(pipeline,
      spec:
        {get_child(:catalog)
         |> via_out(Pad.ref(:output, :blocked),
           options: [track: ref, stream_format: %Track{packaging: "opus", initialization: nil}]
         )
         |> child(:consumer, Testing.Sink), group: :selected_media, crash_group_mode: :temporary}
    )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_source, ^ref,
       {:subscription_failed, ^ref,
        %MOQX.ProtocolError{protocol: :moq_lite_05, operation: :subscribe, code: 0x10}}}
    )

    assert_child_terminated(pipeline, :catalog)
    assert_child_terminated(pipeline, :consumer)
    assert Process.alive?(pipeline)
    Testing.Pipeline.terminate(pipeline)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "reports rejected HANG catalog subscription and removes the failed catalog child" do
    alias Membrane.MOQX.{Sink, TestLiteBridge}
    relay = TestLiteBridge.start(reject_track: "catalog.json")
    namespace = ["room", "rejected.hang"]

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:publisher, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :publisher, {:publication_ready, ^namespace})

    catalog = %CatalogSource{
      endpoint: relay.subscriber_endpoint,
      protocol: :moq_lite_05,
      profile: :hang,
      namespace: namespace,
      transport: relay.transport
    }

    spec = {child(:catalog, catalog), group: :catalog_reader, crash_group_mode: :temporary}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:catalog_failed,
       %MOQX.ProtocolError{protocol: :moq_lite_05, operation: :subscribe, code: 0x10}}
    )

    assert_child_terminated(pipeline, :catalog)
    assert Process.alive?(pipeline)
    Testing.Pipeline.terminate(pipeline)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "reports HANG connection loss and permits a fresh catalog child in the same pipeline" do
    alias Membrane.MOQX.{Sink, TestLiteBridge}
    pipeline = Testing.Pipeline.start_link_supervised!(spec: [])

    for generation <- 1..2 do
      relay = TestLiteBridge.start()
      namespace = ["room", "empty-#{generation}.hang"]

      publisher =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:publisher, %Sink{
              endpoint: relay.publisher_endpoint,
              protocol: :moq_lite_05,
              profile: :hang,
              namespace: namespace,
              transport: relay.transport
            })
        )

      assert_pipeline_notified(publisher, :publisher, {:publication_ready, ^namespace})

      catalog = %CatalogSource{
        endpoint: relay.subscriber_endpoint,
        protocol: :moq_lite_05,
        profile: :hang,
        namespace: namespace,
        transport: relay.transport
      }

      spec = {child(:catalog, catalog), group: :catalog_reader, crash_group_mode: :temporary}
      Testing.Pipeline.execute_actions(pipeline, spec: spec)
      assert_pipeline_notified(pipeline, :catalog, :catalog_ready)
      refute_pipeline_notified(pipeline, :catalog, {:track_available, _offer}, 100)

      :ok = TestLiteBridge.disconnect_subscriber(relay, 99)
      assert_pipeline_notified(pipeline, :catalog, {:connection_closed, %{error_code: 99}})
      assert_child_terminated(pipeline, :catalog)
      assert Process.alive?(pipeline)
      Testing.Pipeline.terminate(publisher)
      assert :ok = TestLiteBridge.stop(relay)
    end

    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "keeps valid HANG offers through malformed snapshots and resumes catalog updates" do
    alias Membrane.MOQX.TestLite05Publisher
    namespace = ["room", "recover.hang"]
    ref = %MOQX.TrackRef{namespace: namespace, track: "catalog.json"}

    valid =
      ~s({"audio":{"renditions":{"audio":{"codec":"opus","sampleRate":48000,"numberOfChannels":2}}}})

    publisher =
      TestLite05Publisher.start(ref, 1_000_000, [
        [{0, valid}],
        [{0, "invalid JSON"}],
        [{0, valid}],
        [{0, "{}"}]
      ])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: TestLite05Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(pipeline, :catalog, {:track_available, %TrackOffer{} = offer})

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:catalog_failed, %MOQX.Catalog.Error{reason: :invalid_json}}
    )

    # The later empty valid snapshot removes the original offer. Repeating the
    # previous valid snapshot after the error must not emit a fresh offer.
    assert_pipeline_notified(pipeline, :catalog, {:track_unavailable, ^offer})
    refute_pipeline_notified(pipeline, :catalog, {:track_available, _offer}, 100)
    refute_pipeline_notified(pipeline, :catalog, {:track_unavailable, _offer}, 100)
    assert Process.alive?(pipeline)

    TestLite05Publisher.finish_group(publisher)
    TestLite05Publisher.finish_subscription(publisher)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "invalidates an old offer when its replacement codec is unsupported" do
    alias Membrane.MOQX.TestLite05Publisher
    namespace = ["room", "codec-change.hang"]
    ref = %MOQX.TrackRef{namespace: namespace, track: "catalog.json"}

    initial =
      ~s({"audio":{"renditions":{"audio":{"codec":"opus","sampleRate":48000,"numberOfChannels":2}}}})

    replacement =
      ~s({"audio":{"renditions":{"audio":{"codec":"unknown-codec","sampleRate":48000,"numberOfChannels":2}}}})

    publisher = TestLite05Publisher.start(ref, 1_000_000, [[{0, initial}], [{0, replacement}]])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: TestLite05Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(pipeline, :catalog, {:track_available, %TrackOffer{} = offer})

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_ignored, "audio", {:unsupported_media, :unknown_codec}}
    )

    assert_pipeline_notified(pipeline, :catalog, {:track_unavailable, ^offer})
    TestLite05Publisher.finish_group(publisher)
    TestLite05Publisher.finish_subscription(publisher)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "withdraws the actual cross-broadcast HANG CMAF offer when its rendition disappears" do
    alias Membrane.MOQX.TestLite05Publisher
    namespace = ["room", "catalog.hang"]
    ref = %MOQX.TrackRef{namespace: namespace, track: "catalog.json"}

    payload =
      ~s({"video":{"renditions":{"video":{"broadcast":"other/media.hang","codec":"avc1.640028","codedWidth":1920,"codedHeight":1080,"container":{"kind":"cmaf","init":"aW5pdA=="}}}}})

    publisher = TestLite05Publisher.start(ref, 1_000_000, [[{0, payload}], [{0, "{}"}]])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: TestLite05Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_available,
       %TrackOffer{
         track_ref: %MOQX.TrackRef{namespace: ["room", "other", "media.hang"], track: "video"},
         stream_format: %Track{packaging: "cmaf", initialization: "init"}
       } = offer}
    )

    assert_pipeline_notified(pipeline, :catalog, {:track_unavailable, ^offer})
    TestLite05Publisher.finish_group(publisher)
    TestLite05Publisher.finish_subscription(publisher)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "reports a malformed HANG catalog without terminating its pipeline" do
    alias Membrane.MOQX.TestLite05Publisher
    namespace = ["room", "invalid.hang"]
    ref = %MOQX.TrackRef{namespace: namespace, track: "catalog.json"}
    publisher = TestLite05Publisher.start(ref, 1_000_000, [{0, "invalid JSON"}])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: TestLite05Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:catalog_failed, %MOQX.Catalog.Error{reason: :invalid_json}}
    )

    assert Process.alive?(pipeline)
    TestLite05Publisher.finish_group(publisher)
    TestLite05Publisher.finish_subscription(publisher)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "offers HANG Opus media from an explicitly selected Lite catalog profile" do
    alias Membrane.MOQX.TestLite05Publisher

    namespace = ["room", "speaker.hang"]
    catalog_ref = %MOQX.TrackRef{namespace: namespace, track: "catalog.json"}

    payload =
      ~s({"audio":{"renditions":{"opus":{"codec":"opus","sampleRate":48000,"numberOfChannels":2}}}})

    publisher = TestLite05Publisher.start(catalog_ref, 1_000_000, [{0, payload}])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            transport: TestLite05Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(pipeline, :catalog, :catalog_ready)

    assert_pipeline_notified(
      pipeline,
      :catalog,
      {:track_available,
       %TrackOffer{
         track_ref: %MOQX.TrackRef{namespace: ^namespace, track: "opus"},
         stream_format: %Track{
           packaging: "hang/legacy",
           initialization: nil,
           selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
         }
       }}
    )

    TestLite05Publisher.finish_group(publisher)
    TestLite05Publisher.finish_subscription(publisher)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestLite05Publisher.await_shutdown(publisher)
  end

  test "requires an explicit catalog profile independently of the protocol" do
    options = %CatalogSource{
      endpoint: "moql://localhost:443",
      protocol: :moq_lite_05,
      namespace: ["live"]
    }

    Process.flag(:trap_exit, true)

    assert {:error, {%Membrane.ParentError{message: message}, _stack}} =
             Testing.Pipeline.start_link(spec: child(:catalog, options))

    assert message =~ "CatalogSource requires a catalog profile"
  end

  test "uses the draft-16 catalog convention and preserves inline initialization" do
    namespace = ["moqtail", "pipeline"]
    media_ref = %MOQX.TrackRef{namespace: namespace, track: "video"}
    initialization = "inline-cmaf-init"

    catalog =
      JSON.encode!(%{
        "version" => 1,
        "tracks" => [
          %{
            "name" => "video",
            "role" => "video",
            "packaging" => "cmaf",
            "codec" => "avc1.42C01F",
            "width" => 640,
            "height" => 360,
            "timescale" => 90_000,
            "initData" => Base.encode64(initialization)
          }
        ]
      })

    publisher = TestDraft16Publisher.start(namespace, catalog, "video", "fragment")

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :draft_16,
            profile: :moqtail_cmsf,
            namespace: namespace,
            transport: TestDraft16Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(pipeline, :source, :catalog_ready)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_available,
       %TrackOffer{
         track_ref: ^media_ref,
         stream_format:
           %Track{
             packaging: "cmaf",
             initialization: ^initialization,
             catalog_fields: %{
               "role" => "video",
               "codec" => "avc1.42C01F",
               "width" => 640,
               "height" => 360,
               "timescale" => 90_000
             }
           } = stream_format
       }}
    )

    pad = Pad.ref(:output, :video)

    link =
      get_child(:source)
      |> via_out(pad, options: [track: media_ref])
      |> child(:sink, %Testing.Sink{})

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: link)
    assert_sink_stream_format(pipeline, :sink, ^stream_format)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_source, ^media_ref, {:subscription_ready, ^media_ref}}
    )

    TestDraft16Publisher.publish_media(publisher)
    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "fragment"})

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestDraft16Publisher.await_shutdown(publisher)
  end

  test "emits a completed draft-16 datagram group when the next group arrives" do
    namespace = ["moqtail", "datagrams"]

    catalog =
      JSON.encode!(%{
        "version" => 1,
        "tracks" => [
          %{
            "name" => "video",
            "role" => "video",
            "packaging" => "cmaf",
            "codec" => "avc1.42C01F",
            "width" => 640,
            "height" => 360,
            "timescale" => 90_000
          }
        ]
      })

    objects = [
      %MOQX.Object{group_id: 1, object_id: 0, payload: "first"},
      %MOQX.Object{group_id: 2, object_id: 0, payload: "second"}
    ]

    publisher =
      TestDraft16Publisher.start(namespace, catalog, "video", objects, delivery: :datagram)

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :draft_16,
            profile: :moqtail_cmsf,
            namespace: namespace,
            transport: TestDraft16Publisher.transport(publisher)
          })
      )

    assert_pipeline_notified(pipeline, :source, :catalog_ready)

    link =
      get_child(:source)
      |> via_out(Pad.ref(:output, :video),
        options: [track: %MOQX.TrackRef{namespace: namespace, track: "video"}]
      )
      |> child(:sink, %Testing.Sink{})

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: link)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_source, track_ref, {:subscription_ready, track_ref}}
    )

    TestDraft16Publisher.publish_media(publisher)
    assert :ok = TestDraft16Publisher.await_datagrams(publisher)

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "first",
        metadata: %{
          moqx: %Unit{group_id: 1, subgroup_id: nil, object_id: 0, group_end?: true}
        }
      }
    )

    TestDraft16Publisher.finish_media(publisher)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestDraft16Publisher.await_shutdown(publisher)
  end

  test "offers catalog tracks and subscribes only when the exact pad is linked" do
    namespace = ["live", "catalog-source"]
    catalog_ref = %MOQX.TrackRef{namespace: namespace, track: ".catalog"}
    media_ref = %MOQX.TrackRef{namespace: namespace, track: "captions"}

    catalog =
      JSON.encode!(%{
        "version" => 1,
        "tracks" => [
          %{
            "name" => "captions",
            "packaging" => "webvtt",
            "selectionParams" => %{"mimeType" => "text/vtt"}
          }
        ]
      })

    publisher =
      TestPublisher.start_many([
        {catalog_ref, [%MOQX.Object{group_id: 0, object_id: 0, payload: catalog}]},
        {media_ref, [%MOQX.Object{group_id: 4, object_id: 0, payload: "cue"}]}
      ])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    spec =
      child(:source, %CatalogSource{
        endpoint: publisher.endpoint,
        protocol: :cloudflare_draft_14,
        profile: :cloudflare_cmsf,
        namespace: namespace,
        transport: TestPublisher.transport(publisher)
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(pipeline, :source, :catalog_ready)

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_available,
       %TrackOffer{
         track_ref: ^media_ref,
         stream_format: %Track{packaging: "webvtt"} = stream_format
       }}
    )

    refute_receive {Testing.Pipeline, ^pipeline,
                    {:handle_child_notification, {{:buffer, _buffer}, :sink}}}

    pad = Pad.ref(:output, :captions)

    link =
      get_child(:source)
      |> via_out(pad,
        options: [
          track: media_ref,
          subscription_options: [delivery_timeout: 50]
        ]
      )
      |> child(:sink, %Testing.Sink{})

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: link)
    assert_pipeline_notified(pipeline, :source, {:track_requested, ^pad, ^media_ref})
    assert_sink_stream_format(pipeline, :sink, ^stream_format)

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "cue",
        metadata: %{moqx: %Unit{group_end?: true, group_id: 4, object_id: 0}}
      }
    )

    assert_end_of_stream(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end

  test "subscribes to a caller-described track even when it is absent from the catalog" do
    namespace = ["live", "pull-source"]
    catalog_ref = %MOQX.TrackRef{namespace: namespace, track: ".catalog"}
    requested_ref = %MOQX.TrackRef{namespace: namespace, track: "on-demand"}
    stream_format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      TestPublisher.start_many([
        {catalog_ref,
         [
           %MOQX.Object{
             group_id: 0,
             object_id: 0,
             payload: JSON.encode!(%{"version" => 1, "tracks" => []})
           }
         ]},
        {requested_ref, [%MOQX.Object{group_id: 9, object_id: 0, payload: "generated"}]}
      ])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pad = Pad.ref(:output, :requested)

    spec =
      child(:source, %CatalogSource{
        endpoint: publisher.endpoint,
        protocol: :cloudflare_draft_14,
        profile: :cloudflare_cmsf,
        namespace: namespace,
        transport: TestPublisher.transport(publisher)
      })
      |> via_out(pad,
        options: [
          track: requested_ref,
          stream_format: stream_format,
          subscription_options: [delivery_timeout: 50]
        ]
      )
      |> child(:sink, %Testing.Sink{})

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(pipeline, :source, {:track_requested, ^pad, ^requested_ref})
    assert_sink_stream_format(pipeline, :sink, ^stream_format)

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "generated",
        metadata: %{moqx: %Unit{group_end?: true, group_id: 9, object_id: 0}}
      }
    )

    assert_end_of_stream(pipeline, :sink)
    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end

  test "reports catalog withdrawal without deciding the attached media lifecycle" do
    namespace = ["live", "catalog-updates"]
    catalog_ref = %MOQX.TrackRef{namespace: namespace, track: ".catalog"}

    present =
      JSON.encode!(%{
        "version" => 1,
        "tracks" => [%{"name" => "events", "packaging" => "application/example"}]
      })

    removed = JSON.encode!(%{"version" => 1, "tracks" => []})

    publisher =
      TestPublisher.start(catalog_ref, [
        %MOQX.Object{group_id: 0, object_id: 0, payload: present},
        %MOQX.Object{group_id: 1, object_id: 0, payload: removed}
      ])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %CatalogSource{
            endpoint: publisher.endpoint,
            protocol: :cloudflare_draft_14,
            profile: :cloudflare_cmsf,
            namespace: namespace,
            transport: TestPublisher.transport(publisher)
          })
      )

    assert_pipeline_notified(
      pipeline,
      :source,
      {:track_available, %TrackOffer{track_ref: track_ref} = offer}
    )

    assert track_ref.track == "events"
    assert_pipeline_notified(pipeline, :source, {:track_unavailable, ^offer})

    assert :ok = Testing.Pipeline.terminate(pipeline)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end
end
