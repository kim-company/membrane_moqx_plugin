defmodule Membrane.MOQX.TrackAdapterFilterTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.Buffer
  alias Membrane.CMAF.Track, as: CMAFTrack
  alias Membrane.MOQX.{Track, Unit}
  alias Membrane.MOQX.TrackAdapter.CMAF
  alias Membrane.MOQX.TrackAdapter.FromTrack
  alias Membrane.MOQX.TrackAdapter.ToTrack
  alias Membrane.Testing

  test "converts an H264 CMAF stream into the canonical MOQX pad contract" do
    cmaf = %CMAFTrack{
      content_type: :video,
      header: "cmaf-init",
      resolution: {1920, 1080},
      codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "1F"}}
    }

    buffer = %Buffer{
      payload: "unchanged-cmaf-segment",
      metadata: %{last_chunk?: true, independent?: true, duration: 2_000}
    }

    spec =
      child(:source, %Testing.Source{
        output: Testing.Source.output_from_buffers([buffer]),
        stream_format: cmaf
      })
      |> child(:adapter, %ToTrack{adapter: CMAF})
      |> child(:sink, Testing.Sink)

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_stream_format(
      pipeline,
      :sink,
      %Track{
        packaging: "cmaf",
        initialization: "cmaf-init",
        selection_params: %{
          "codec" => "avc1.42C01F",
          "mimeType" => "video/mp4",
          "width" => 1920,
          "height" => 1080
        }
      }
    )

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "unchanged-cmaf-segment",
        metadata: %{
          moqx: %Unit{
            group_end?: true
          }
        }
      }
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "converts the canonical MOQX pad contract back into H264 CMAF" do
    track = %Track{
      packaging: "cmaf",
      initialization: "cmaf-init",
      selection_params: %{
        "codec" => "avc1.42C01F",
        "mimeType" => "video/mp4",
        "width" => 1920,
        "height" => 1080
      }
    }

    unit = %Unit{
      group_end?: true,
      group_id: 7,
      subgroup_id: 0,
      object_id: 3,
      publisher_priority: 127
    }

    buffer = %Buffer{payload: "unchanged-cmaf-segment", metadata: %{moqx: unit}}

    spec =
      child(:source, %Testing.Source{
        output: Testing.Source.output_from_buffers([buffer]),
        stream_format: track
      })
      |> child(:adapter, %FromTrack{adapter: CMAF})
      |> child(:sink, Testing.Sink)

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_stream_format(
      pipeline,
      :sink,
      %CMAFTrack{
        content_type: :video,
        header: "cmaf-init",
        resolution: {1920, 1080},
        codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "1F"}}
      }
    )

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "unchanged-cmaf-segment",
        metadata: %{
          moqx: ^unit,
          last_chunk?: true
        }
      }
    )

    assert :ok = Testing.Pipeline.terminate(pipeline)
  end
end
