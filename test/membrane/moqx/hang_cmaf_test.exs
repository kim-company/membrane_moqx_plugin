defmodule Membrane.MOQX.HangCMAFTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.Buffer
  alias Membrane.MOQX.Hang.CMAF
  alias Membrane.MOQX.{Track, Unit}
  alias Membrane.MOQX.TrackAdapter.{FromTrack, ToTrack}
  alias Membrane.Testing

  test "CMAF composition preserves empty-group epochs without rewriting chunks or timestamps" do
    format = %Membrane.CMAF.Track{
      content_type: :audio,
      header: "init",
      codecs: %{mp4a: %{aot_id: "2", frequency: 48_000, channels: 2}}
    }

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Membrane.MOQX.TestControlledSource{stream_format: format})
          |> child(:encode, %ToTrack{adapter: CMAF})
          |> child(:decode, %FromTrack{adapter: CMAF})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(pipeline, :sink, ^format)

    Testing.Pipeline.notify_child(
      pipeline,
      :source,
      {:publish,
       [
         %Buffer{payload: "old-moof-mdat", pts: 50_000, metadata: %{last_chunk?: true}}
       ]}
    )

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "old-moof-mdat", pts: 50_000})

    Testing.Pipeline.notify_child(
      pipeline,
      :source,
      {:event, %Membrane.MOQX.Event.EmptyGroup{group_id: 1}}
    )

    assert_sink_event(pipeline, :sink, %Membrane.MOQX.Event.EmptyGroup{group_id: 1})

    Testing.Pipeline.notify_child(
      pipeline,
      :source,
      {:publish,
       [
         %Buffer{payload: "new-moof-mdat", pts: 1_000, metadata: %{last_chunk?: true}}
       ]}
    )

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: "new-moof-mdat",
      pts: 1_000,
      metadata: %{last_chunk?: true}
    })

    Testing.Pipeline.notify_child(pipeline, :source, :end_of_stream)
    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "AAC CMAF uses HANG decoder keys and restores sample rate and channels" do
    format = %Membrane.CMAF.Track{
      content_type: :audio,
      header: "aac-init",
      codecs: %{mp4a: %{aot_id: "2", frequency: 48_000, channels: 2}}
    }

    buffer = %Buffer{payload: "aac-moof-mdat", pts: 42_000_000, metadata: %{last_chunk?: true}}

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers([buffer])
          })
          |> child(:adapter, %ToTrack{adapter: CMAF})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(
      pipeline,
      :sink,
      %Track{
        packaging: "cmaf",
        initialization: "aac-init",
        selection_params: %{
          "codec" => "mp4a.40.2",
          "sampleRate" => 48_000,
          "numberOfChannels" => 2
        },
        catalog_fields: %{"role" => "audio"}
      } = track
    )

    assert_sink_buffer(
      pipeline,
      :sink,
      %Buffer{
        payload: "aac-moof-mdat",
        pts: 42_000_000,
        metadata: %{moqx: %Unit{group_end?: true}}
      } = received
    )

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)

    receiver =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: track,
            output: Testing.Source.output_from_buffers([received])
          })
          |> child(:adapter, %FromTrack{adapter: CMAF})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(receiver, :sink, ^format)

    assert_sink_buffer(receiver, :sink, %Buffer{
      payload: "aac-moof-mdat",
      pts: 42_000_000,
      metadata: %{last_chunk?: true}
    })

    assert_end_of_stream(receiver, :sink)
    Testing.Pipeline.terminate(receiver)
  end

  test "restores HANG CMAF video initialization and dimensions without retiming chunks" do
    track = %Track{
      packaging: "cmaf",
      initialization: "init",
      selection_params: %{"codec" => "avc1.640028", "codedWidth" => 1920, "codedHeight" => 1080},
      catalog_fields: %{"role" => "video", "container" => %{"kind" => "cmaf"}}
    }

    buffer = %Buffer{
      payload: "moof-mdat",
      pts: 1_500_000,
      metadata: %{moqx: %Unit{group_end?: false}}
    }

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: track,
            output: Testing.Source.output_from_buffers([buffer])
          })
          |> child(:adapter, %FromTrack{adapter: CMAF})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(pipeline, :sink, %Membrane.CMAF.Track{
      header: "init",
      content_type: :video,
      resolution: {1920, 1080},
      codecs: %{avc1: %{profile: "64", compatibility: "00", level: "28"}}
    })

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: "moof-mdat",
      pts: 1_500_000,
      metadata: %{last_chunk?: false, moqx: %Unit{group_end?: false}}
    })

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end
end
