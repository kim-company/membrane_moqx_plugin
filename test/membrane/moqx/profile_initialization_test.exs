defmodule Membrane.MOQX.ProfileInitializationTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Pad, Testing}

  alias Membrane.MOQX.{
    CatalogSource,
    Sink,
    TestControlledSource,
    TestLiteBridge,
    Track,
    TrackOffer
  }

  require Pad

  test "Cloudflare profile publishes separate initialization before its draft-16 catalog" do
    alias Membrane.MOQX.TestDraft16Relay
    namespace = ["profiles", "cloudflare-over-draft16"]
    relay = TestDraft16Relay.start_initialized(namespace, "video")

    format = %Track{
      packaging: "cmaf",
      initialization: "draft16-init",
      selection_params: %{"codec" => "avc1.42001e"}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :video), options: [track_name: "video"])
          |> child(:sink, %Sink{
            endpoint: relay.endpoint,
            protocol: :draft_16,
            profile: :cloudflare_cmsf,
            namespace: namespace,
            transport: TestDraft16Relay.transport(relay)
          })
      )

    for name <- [".catalog", "video.init", "video"] do
      assert :ok = TestDraft16Relay.await_pending(relay, name)
      TestDraft16Relay.ready(relay, name)
    end

    assert {:ok, captured} = TestDraft16Relay.capture(relay)
    assert captured.initialization.payload == "draft16-init"

    assert %{"tracks" => [%{"initTrack" => "video.init"}]} =
             JSON.decode!(captured.catalog.payload)

    Testing.Pipeline.notify_child(
      publisher,
      :producer,
      {:stream_format, %{format | initialization: "draft16-init-v2"}}
    )

    assert :ok = TestDraft16Relay.await_pending(relay, "video.init.1")
    refute_pipeline_notified(publisher, :sink, {:track_updated, _, "video", 1}, 50)
    TestDraft16Relay.ready(relay, "video.init.1")
    assert {:ok, updated} = TestDraft16Relay.capture(relay)
    assert updated.initialization.payload == "draft16-init-v2"

    assert %{"tracks" => [%{"initTrack" => "video.init.1"}]} =
             JSON.decode!(updated.catalog.payload)

    Testing.Pipeline.terminate(publisher)
    assert :ok = TestDraft16Relay.await_shutdown(relay)
  end

  for profile <- [:cloudflare_cmsf, :moqtail_cmsf] do
    @profile profile
    test "#{profile} delivers initial and updated initialization over Lite" do
      relay = TestLiteBridge.start()
      namespace = ["profiles", "cloudflare-over-lite"]

      format = %Track{
        packaging: "cmaf",
        initialization: "separate-init",
        catalog_fields: %{"role" => "video"},
        selection_params: %{"codec" => "avc1.42001e"}
      }

      publisher =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:producer, %TestControlledSource{stream_format: format})
            |> via_in(Pad.ref(:input, :video),
              options: [track_name: "video", timescale: 1_000_000]
            )
            |> child(:sink, %Sink{
              endpoint: relay.publisher_endpoint,
              protocol: :moq_lite_05,
              profile: @profile,
              namespace: namespace,
              transport: relay.transport
            })
        )

      assert_pipeline_notified(publisher, :sink, {:track_ready, _, "video"})

      subscriber =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:catalog, %CatalogSource{
              endpoint: relay.subscriber_endpoint,
              protocol: :moq_lite_05,
              profile: @profile,
              namespace: namespace,
              transport: relay.transport
            })
        )

      assert_pipeline_notified(subscriber, :catalog, {:track_available, %TrackOffer{} = offer})
      assert offer.stream_format.initialization == "separate-init"
      assert offer.track_ref == %MOQX.TrackRef{namespace: namespace, track: "video"}
      updated = %{format | initialization: "updated-init"}
      Testing.Pipeline.notify_child(publisher, :producer, {:stream_format, updated})
      assert_pipeline_notified(publisher, :sink, {:track_updated, _, "video", 1})

      assert_pipeline_notified(
        subscriber,
        :catalog,
        {:track_available, %TrackOffer{} = updated_offer}
      )

      assert updated_offer.stream_format.initialization == "updated-init"
      assert updated_offer.track_ref == offer.track_ref
      Testing.Pipeline.terminate(subscriber)
      Testing.Pipeline.terminate(publisher)
      assert :ok = TestLiteBridge.stop(relay)
    end
  end
end
