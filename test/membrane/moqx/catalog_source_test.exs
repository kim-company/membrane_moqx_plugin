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
