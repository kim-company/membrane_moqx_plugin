defmodule Membrane.MOQX.ProtocolConventionsTest do
  use ExUnit.Case, async: true

  alias Membrane.MOQX.{ProtocolConventions, Track}

  @track %Track{
    packaging: "cmaf",
    initialization: <<0, 1, 2, 3>>,
    selection_params: %{
      "codec" => "avc1.42C01F",
      "width" => 640,
      "height" => 360
    },
    catalog_fields: %{
      "role" => "video",
      "bitrate" => 800_000,
      "timescale" => 90_000
    }
  }

  test "draft-16 resolves Moqtail publication conventions literally" do
    assert ProtocolConventions.catalog_track_name(:draft_16, nil) == "catalog"
    assert ProtocolConventions.initialization_mode(:draft_16) == :inline

    assert ProtocolConventions.track_options(:draft_16, :all, :datagram) ==
             [retention: :all, delivery: :datagram]

    assert ProtocolConventions.catalog_priority(:draft_16, 127) == 0

    assert ProtocolConventions.catalog(:draft_16, ["live", "camera"], [
             {"video", nil, @track}
           ]) == %{
             "version" => 1,
             "tracks" => [
               %{
                 "name" => "video",
                 "role" => "video",
                 "packaging" => "cmaf",
                 "codec" => "avc1.42C01F",
                 "width" => 640,
                 "height" => 360,
                 "bitrate" => 800_000,
                 "timescale" => 90_000,
                 "initData" => Base.encode64(@track.initialization)
               }
             ]
           }
  end

  test "Cloudflare draft-14 keeps its deployed catalog and initialization conventions" do
    assert ProtocolConventions.catalog_track_name(:cloudflare_draft_14, nil) == ".catalog"
    assert ProtocolConventions.initialization_mode(:cloudflare_draft_14) == :separate_track

    assert ProtocolConventions.track_options(:cloudflare_draft_14, :live, :subgroup) ==
             [retention: :live]

    assert ProtocolConventions.catalog_priority(:cloudflare_draft_14, 127) == 127

    assert ProtocolConventions.catalog(:cloudflare_draft_14, ["live", "camera"], [
             {"video", "video.init", @track}
           ]) == %{
             "version" => 1,
             "streamingFormat" => 1,
             "streamingFormatVersion" => "0.2",
             "supportsDeltaUpdates" => false,
             "commonTrackFields" => %{"namespace" => "live/camera"},
             "tracks" => [
               %{
                 "name" => "video",
                 "packaging" => "cmaf",
                 "selectionParams" => %{
                   "codec" => "avc1.42C01F",
                   "width" => 640,
                   "height" => 360
                 },
                 "role" => "video",
                 "bitrate" => 800_000,
                 "timescale" => 90_000,
                 "initTrack" => "video.init"
               }
             ]
           }
  end

  test "MoQ Lite draft-05 uses exact tracks with no catalog or initialization track" do
    assert ProtocolConventions.moq_lite_05?(:moq_lite_05)
    assert ProtocolConventions.moq_lite_05?(MOQX.Protocol.MOQLite05)
    assert ProtocolConventions.catalog_track_name(:moq_lite_05, nil) == nil
    assert ProtocolConventions.initialization_mode(:moq_lite_05) == :none
    assert ProtocolConventions.catalog(:moq_lite_05, ["live"], [{"video", nil, @track}]) == nil

    assert ProtocolConventions.track_options(
             :moq_lite_05,
             :latest,
             :subgroup,
             timescale: 48_000,
             publisher_priority: 17,
             publisher_max_latency: 1_000
           ) == [
             retention: :latest,
             delivery: :subgroup,
             timescale: 48_000,
             publisher_priority: 17,
             publisher_max_latency: 1_000
           ]
  end

  test "an explicit catalog track override wins without endpoint inference" do
    assert ProtocolConventions.catalog_track_name(:draft_16, ".custom") == ".custom"
    assert ProtocolConventions.catalog_track_name(:cloudflare_draft_14, "custom") == "custom"
  end
end
