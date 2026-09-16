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

  test "draft-18 resolves Moqtail publication conventions literally" do
    assert ProtocolConventions.track_options(:draft_18, :all, :datagram) ==
             [retention: :all, delivery: :datagram]

    assert ProtocolConventions.catalog_priority(:draft_18, 127) == 0

    assert ProtocolConventions.catalog(:moqtail_cmsf, ["live", "camera"], [
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

  test "the Cloudflare CMSF profile keeps its deployed catalog and initialization conventions" do
    assert ProtocolConventions.profile_catalog_track_name(:cloudflare_cmsf, :none, nil) ==
             ".catalog"

    assert ProtocolConventions.track_options(:draft_18, :live, :subgroup) ==
             [retention: :live, delivery: :subgroup]

    assert ProtocolConventions.catalog_priority(:draft_18, 127) == 0

    assert ProtocolConventions.catalog(:cloudflare_cmsf, ["live", "camera"], [
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
end
