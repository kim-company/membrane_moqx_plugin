defmodule Membrane.MOQX.CompressedCatalogTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.MOQX.{
    CatalogSource,
    Sink,
    TestControlledSource,
    TestLiteBridge,
    Track,
    TrackOffer
  }

  alias Membrane.{Pad, Testing}

  require Pad

  test "DEFLATE HANG publication is readable by live and late catalog consumers" do
    relay = TestLiteBridge.start()
    namespace = ["catalog", "compressed"]

    format = %Track{
      packaging: "hang/legacy",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2},
      catalog_fields: %{"role" => "audio"}
    }

    sink =
      struct!(Sink,
        endpoint: relay.publisher_endpoint,
        protocol: :moq_lite_05,
        profile: :hang,
        catalog_compression: :deflate,
        namespace: namespace,
        transport: relay.transport,
        catalog_refresh_interval: nil
      )

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> via_in(Pad.ref(:input, :audio), options: [track_name: "audio", timescale: 1_000_000])
          |> child(:publisher, sink)
      )

    assert_pipeline_notified(publisher, :publisher, {:track_ready, _, "audio"})
    first = catalog(relay.subscriber_endpoint, relay, namespace)

    assert_pipeline_notified(
      first,
      :catalog,
      {:track_available, %TrackOffer{stream_format: received}}
    )

    assert received.selection_params == format.selection_params

    updated = %{
      format
      | selection_params: Map.put(format.selection_params, "numberOfChannels", 1)
    }

    Testing.Pipeline.notify_child(publisher, :producer, {:stream_format, updated})

    assert_pipeline_notified(
      first,
      :catalog,
      {:track_available, %TrackOffer{stream_format: received}}
    )

    assert received.selection_params == updated.selection_params
    {endpoint, relay} = TestLiteBridge.add_subscriber_endpoint(relay)
    late = catalog(endpoint, relay, namespace)

    assert_pipeline_notified(
      late,
      :catalog,
      {:track_available, %TrackOffer{stream_format: received}}
    )

    assert received.selection_params == updated.selection_params
    {raw_endpoint, relay} = TestLiteBridge.add_subscriber_endpoint(relay)
    {:ok, raw} = MOQX.connect(raw_endpoint, protocol: :moq_lite_05, transport: relay.transport)

    {:ok, subscription} =
      MOQX.subscribe(
        raw,
        %MOQX.TrackRef{namespace: namespace, track: "catalog.json.z"}
      )

    assert_receive {:moqx, ^raw,
                    %MOQX.Event.ObjectReceived{
                      object: %MOQX.Object{subscription: ^subscription, payload: compressed}
                    }},
                   2_000

    # Independent RFC 7692 raw-DEFLATE decoding, not MOQX's catalog decoder.
    inflater = :zlib.open()
    :ok = :zlib.inflateInit(inflater, -15)
    json = :zlib.inflate(inflater, compressed <> <<0, 0, 255, 255>>) |> IO.iodata_to_binary()
    :zlib.close(inflater)

    assert %{"audio" => %{"renditions" => %{"audio" => %{"numberOfChannels" => 1}}}} =
             JSON.decode!(json)

    assert {:error, _} = JSON.decode(compressed)
    :ok = MOQX.close(raw)
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    for consumer <- [first, late] do
      assert_pipeline_notified(consumer, :catalog, {:track_unavailable, _})
      Testing.Pipeline.terminate(consumer)
    end

    Testing.Pipeline.terminate(publisher)
    assert :ok = TestLiteBridge.stop(relay)
  end

  test "inconsistent catalog names and unsupported encodings fail before connecting" do
    Process.flag(:trap_exit, true)

    for module <- [Sink, CatalogSource],
        {profile, compression, name} <- [
          {:hang, :none, "catalog.json.z"},
          {:hang, :deflate, "catalog.json"},
          {:hang, :deflate, "custom-compressed"},
          {:moqtail_cmsf, :deflate, nil}
        ] do
      options =
        struct!(module,
          endpoint: "moql://localhost:1",
          protocol: :moq_lite_05,
          namespace: ["invalid"],
          profile: profile,
          catalog_compression: compression,
          catalog_track_name: name
        )

      assert {:error, {%Membrane.ParentError{}, _}} =
               Testing.Pipeline.start_link(spec: child(:component, options))
    end
  end

  defp catalog(endpoint, relay, namespace) do
    options =
      struct!(CatalogSource,
        endpoint: endpoint,
        protocol: :moq_lite_05,
        profile: :hang,
        catalog_compression: :deflate,
        namespace: namespace,
        transport: relay.transport
      )

    Testing.Pipeline.start_link_supervised!(spec: child(:catalog, options))
  end
end
