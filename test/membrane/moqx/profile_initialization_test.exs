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

  for next_init <- [nil, "v3"] do
    @next_init next_init
    test "pending initialization preserves media, subsequent #{@next_init || "nil"} format and EOS in input order" do
      alias Membrane.Buffer
      alias Membrane.MOQX.{TestDraft16Relay, Unit}
      namespace = ["profiles", "queued-generations"]
      relay = TestDraft16Relay.start_initialized_queue(namespace, "video", not is_nil(@next_init))
      format = %Track{packaging: "cmaf", initialization: "v1"}

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
              catalog_refresh_interval: nil,
              transport: TestDraft16Relay.transport(relay)
            })
        )

      for name <- [".catalog", "video.init", "video"] do
        assert :ok = TestDraft16Relay.await_pending(relay, name)
        TestDraft16Relay.ready(relay, name)
      end

      assert {:ok, _initial} = TestDraft16Relay.capture(relay)

      Testing.Pipeline.notify_child(
        publisher,
        :producer,
        {:stream_format, %{format | initialization: "v2"}}
      )

      assert :ok = TestDraft16Relay.await_pending(relay, "video.init.1")

      Testing.Pipeline.notify_child(
        publisher,
        :producer,
        {:publish, [%Buffer{payload: "under-v2", metadata: %{moqx: %Unit{group_end?: true}}}]}
      )

      # Repeated current format must not allocate an extra generation on replay.
      Testing.Pipeline.notify_child(
        publisher,
        :producer,
        {:stream_format, %{format | initialization: "v2"}}
      )

      Testing.Pipeline.notify_child(
        publisher,
        :producer,
        {:stream_format, %{format | initialization: @next_init}}
      )

      Testing.Pipeline.notify_child(
        publisher,
        :producer,
        {:stream_format, %{format | initialization: @next_init}}
      )

      Testing.Pipeline.notify_child(
        publisher,
        :producer,
        {:publish, [%Buffer{payload: "under-nil", metadata: %{moqx: %Unit{group_end?: true}}}]}
      )

      Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)
      refute_pipeline_notified(publisher, :sink, {:track_ended, _, "video"}, 100)

      TestDraft16Relay.ready(relay, "video.init.1")
      assert {:ok, generation2} = TestDraft16Relay.capture(relay)
      assert generation2.initialization.payload == "v2"

      assert %{"tracks" => [%{"initTrack" => "video.init.1"}]} =
               JSON.decode!(generation2.catalog.payload)

      if @next_init do
        assert :ok = TestDraft16Relay.await_pending(relay, "video.init.2")
        refute_pipeline_notified(publisher, :sink, {:track_updated, _, "video", 2}, 100)
        refute_pipeline_notified(publisher, :sink, {:track_ended, _, "video"}, 100)
        TestDraft16Relay.ready(relay, "video.init.2")
      end

      assert {:ok, objects} = TestDraft16Relay.capture(relay)

      [media2, catalog3, media3, eos, removed] =
        if @next_init do
          [media2, init3 | rest] = objects
          assert init3.payload == "v3"
          [media2 | rest]
        else
          objects
        end

      assert media2.payload == "under-v2"
      assert %{"tracks" => [track3]} = JSON.decode!(catalog3.payload)

      if @next_init,
        do: assert(track3["initTrack"] == "video.init.2"),
        else: refute(Map.has_key?(track3, "initTrack"))

      assert media3.payload == "under-nil"
      assert eos.status == :end_of_track
      assert %{"tracks" => []} = JSON.decode!(removed.payload)
      assert_pipeline_notified(publisher, :sink, {:track_ended, _, "video"})
      Testing.Pipeline.terminate(publisher)
      assert :ok = TestDraft16Relay.await_shutdown(relay)
    end
  end

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
