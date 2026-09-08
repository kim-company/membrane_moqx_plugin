defmodule Membrane.MOQX.HangCatalogLifecycleTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}

  alias Membrane.MOQX.{
    CatalogSource,
    Sink,
    TestControlledSource,
    TestLiteBridge,
    Track,
    TrackOffer,
    Unit
  }

  require Pad

  test "live and late HANG catalog consumers observe configuration changes and removal" do
    relay = TestLiteBridge.start()
    namespace = ["catalog", "multiple.hang"]

    format = %Track{
      packaging: "hang/legacy",
      initialization: <<1, 66, 0, 30>>,
      selection_params: %{"codec" => "avc1.42001e", "codedWidth" => 640, "codedHeight" => 360},
      catalog_fields: %{"role" => "video"}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:video, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :video), options: [track_name: "video", timescale: 1_000_000])
          |> child(:publisher, %Sink{
            endpoint: relay.publisher_endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            catalog_refresh_interval: nil,
            transport: relay.transport
          })
      )

    assert_pipeline_notified(publisher, :publisher, {:track_ready, _, "video"})
    first = catalog(relay.subscriber_endpoint, relay, namespace)
    {endpoint, relay} = TestLiteBridge.add_subscriber_endpoint(relay)
    second = catalog(endpoint, relay, namespace)
    ref = %MOQX.TrackRef{namespace: namespace, track: "video"}

    for consumer <- [first, second] do
      assert_pipeline_notified(
        consumer,
        :catalog,
        {:track_available, %TrackOffer{track_ref: ^ref, stream_format: received}}
      )

      assert received.initialization == <<1, 66, 0, 30>>
      assert received.selection_params == format.selection_params
    end

    updated = %{
      format
      | initialization: <<1, 77, 0, 31>>,
        selection_params: %{"codec" => "avc1.4d001f", "codedWidth" => 1280, "codedHeight" => 720}
    }

    Testing.Pipeline.notify_child(publisher, :video, {:stream_format, updated})

    for consumer <- [first, second] do
      assert_pipeline_notified(
        consumer,
        :catalog,
        {:track_available, %TrackOffer{track_ref: ^ref, stream_format: received}}
      )

      assert received.initialization == updated.initialization
      assert received.selection_params == updated.selection_params
    end

    {endpoint, relay} = TestLiteBridge.add_subscriber_endpoint(relay)
    late = catalog(endpoint, relay, namespace)

    assert_pipeline_notified(
      late,
      :catalog,
      {:track_available, %TrackOffer{track_ref: ^ref, stream_format: received}}
    )

    assert received.initialization == updated.initialization
    assert received.selection_params == updated.selection_params

    unknown = %{format | initialization: nil, selection_params: %{"codec" => "vendor.future"}}

    Testing.Pipeline.execute_actions(publisher,
      spec:
        child(:unsupported, %TestControlledSource{stream_format: unknown})
        |> via_in(Pad.ref(:input, :unsupported),
          options: [track_name: "unsupported", timescale: 1_000_000]
        )
        |> get_child(:publisher)
    )

    assert_pipeline_notified(publisher, :publisher, {:track_ready, _, "unsupported"})

    for consumer <- [first, second, late] do
      assert_pipeline_notified(
        consumer,
        :catalog,
        {:track_ignored, "unsupported", {:unsupported_media, :unknown_codec}}
      )
    end

    # The unsupported entry does not invalidate the usable offer or prevent selection.
    # This exercises framed-track delivery, not the validity of a decoder bitstream.
    Testing.Pipeline.execute_actions(late,
      spec:
        get_child(:catalog)
        |> via_out(Pad.ref(:output, :video), options: [track: ref])
        |> child(:media, Testing.Sink)
    )

    assert_pipeline_notified(publisher, :publisher, {:subscriber_joined, "video", _, 1})

    Testing.Pipeline.notify_child(
      publisher,
      :video,
      {:publish,
       [
         %Buffer{
           payload: "framed-media",
           pts: 1_000_000,
           metadata: %{moqx: %Unit{group_end?: true}}
         }
       ]}
    )

    assert_sink_stream_format(late, :media, %Track{initialization: <<1, 77, 0, 31>>})
    assert_sink_buffer(late, :media, %Buffer{payload: "framed-media", pts: 1_000_000})
    Testing.Pipeline.execute_actions(late, remove_children: :media)
    assert_pipeline_notified(publisher, :publisher, {:subscriber_left, "video", _, 0})

    Testing.Pipeline.execute_actions(publisher, remove_children: :video)

    for consumer <- [first, second, late] do
      assert_pipeline_notified(
        consumer,
        :catalog,
        {:track_unavailable, %TrackOffer{track_ref: ^ref, stream_format: removed}}
      )

      assert removed.initialization == updated.initialization
      assert removed.selection_params == updated.selection_params
    end

    for consumer <- [first, second, late], do: Testing.Pipeline.terminate(consumer)
    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  defp catalog(endpoint, relay, namespace) do
    Testing.Pipeline.start_link_supervised!(
      spec:
        child(:catalog, %CatalogSource{
          endpoint: endpoint,
          protocol: :moq_lite_05,
          profile: :hang,
          namespace: namespace,
          transport: relay.transport
        })
    )
  end
end
