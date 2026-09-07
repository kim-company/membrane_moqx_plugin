defmodule Membrane.MOQX.HangLegacyTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.Buffer
  alias Membrane.MOQX.Hang.Legacy
  alias Membrane.MOQX.{Track, Unit}
  alias Membrane.Testing

  test "uses received object coordinates for keyframes across interleaved groups" do
    format = %Track{
      packaging: "hang/legacy",
      initialization: nil,
      selection_params: %{"codec" => "avc1.42001e"}
    }

    buffers = [
      %Buffer{
        payload: <<0, "a-key">>,
        metadata: %{moqx: %Unit{group_id: 7, object_id: 0, group_end?: false}}
      },
      %Buffer{
        payload: <<1, "b-key">>,
        metadata: %{moqx: %Unit{group_id: 8, object_id: 0, group_end?: true}}
      },
      %Buffer{
        payload: <<2, "a-delta">>,
        metadata: %{moqx: %Unit{group_id: 7, object_id: 1, group_end?: true}}
      }
    ]

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers(buffers)
          })
          |> child(:decode, %Legacy{direction: :decode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "a-key", metadata: %{keyframe?: true}})
    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "b-key", metadata: %{keyframe?: true}})

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "a-delta", metadata: %{keyframe?: false}})

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "decoded H264 keyframe metadata permits lossless explicit reframing" do
    format = %Track{
      packaging: "hang/legacy",
      initialization: <<1, 66, 0, 30>>,
      selection_params: %{"codec" => "avc1.42001e"},
      catalog_fields: %{"role" => "video"}
    }

    buffers = [
      %Buffer{payload: <<0, "key">>, pts: 9_000, metadata: %{moqx: %Unit{group_end?: false}}},
      %Buffer{payload: <<1, "delta">>, pts: 9_000, metadata: %{moqx: %Unit{group_end?: true}}},
      %Buffer{payload: <<2, "next-key">>, pts: 9_000, metadata: %{moqx: %Unit{group_end?: true}}}
    ]

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers(buffers)
          })
          |> child(:decode, %Legacy{direction: :decode})
          |> child(:encode, %Legacy{direction: :encode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<0, "key">>,
      pts: 0,
      metadata: %{keyframe?: true, moqx: %Unit{group_end?: false}}
    })

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<1, "delta">>,
      pts: 1_000,
      metadata: %{keyframe?: false, moqx: %Unit{group_end?: true}}
    })

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<2, "next-key">>,
      pts: 2_000,
      metadata: %{keyframe?: true, moqx: %Unit{group_end?: true}}
    })

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "re-encodes an explicit media endpoint without losing its group boundary" do
    format = %Track{
      packaging: "hang/legacy",
      initialization: nil,
      selection_params: %{"codec" => "opus"}
    }

    unit = %Unit{group_end?: true}
    buffer = %Buffer{payload: <<0x45, 0xDC>>, pts: 9_000_000, metadata: %{moqx: unit}}

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers([buffer])
          })
          |> child(:decode, %Legacy{direction: :decode})
          |> child(:encode, %Legacy{direction: :encode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<0x45, 0xDC>>,
      pts: 1_500_000,
      metadata: %{moqx: ^unit}
    })

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "rejects a video group that begins with a delta access unit" do
    format = %Track{
      packaging: "h264",
      initialization: nil,
      selection_params: %{"codec" => "avc1.42001e"}
    }

    buffer = %Buffer{
      payload: "delta",
      pts: 0,
      metadata: %{keyframe?: false, moqx: %Unit{group_end?: true}}
    }

    spec =
      child(:source, %Testing.Source{
        stream_format: format,
        output: Testing.Source.output_from_buffers([buffer])
      })
      |> child(:framing, %Legacy{direction: :encode})
      |> child(:sink, Testing.Sink)

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec: {spec, group: :invalid_video, crash_group_mode: :temporary}
      )

    assert_pipeline_crash_group_down(pipeline, :invalid_video)
    refute_sink_buffer(pipeline, :sink, %Buffer{}, 20)
    Testing.Pipeline.terminate(pipeline)
  end

  test "decodes H264 framing without stripping or rewriting codec configuration" do
    format = %Track{
      packaging: "hang/legacy",
      initialization: <<1, 100, 0, 31>>,
      selection_params: %{"codec" => "avc1.64001f"},
      catalog_fields: %{"role" => "video"}
    }

    buffer = %Buffer{
      payload: <<0, "access unit">>,
      pts: 9_000,
      metadata: %{moqx: %Unit{group_end?: true}}
    }

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers([buffer])
          })
          |> child(:framing, %Legacy{direction: :decode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(pipeline, :sink, %Track{
      packaging: "h264",
      initialization: <<1, 100, 0, 31>>,
      selection_params: %{"codec" => "avc1.64001f"}
    })

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "access unit", pts: 0})
    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "frames H264 access units while preserving decoder initialization and group boundaries" do
    format = %Track{
      packaging: "h264",
      initialization: <<1, 100, 0, 31>>,
      selection_params: %{"codec" => "avc1.64001f", "codedWidth" => 1280, "codedHeight" => 720}
    }

    buffers = [
      %Buffer{
        payload: "key",
        pts: 0,
        metadata: %{keyframe?: true, moqx: %Unit{group_end?: false}}
      },
      %Buffer{
        payload: "delta",
        pts: 1_000,
        metadata: %{keyframe?: false, moqx: %Unit{group_end?: true}}
      }
    ]

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers(buffers)
          })
          |> child(:framing, %Legacy{direction: :encode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(pipeline, :sink, %Track{
      packaging: "hang/legacy",
      initialization: <<1, 100, 0, 31>>,
      catalog_fields: %{"role" => "video"}
    })

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<0, "key">>,
      metadata: %{moqx: %Unit{group_end?: false}}
    })

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<1, "delta">>,
      metadata: %{moqx: %Unit{group_end?: true}}
    })

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "reports a media endpoint without feeding the marker to a decoder or dropping flush packets" do
    format = %Track{
      packaging: "hang/legacy",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    buffers = [
      %Buffer{payload: <<0x45, 0xDC>>, pts: 0, metadata: %{moqx: %Unit{group_end?: false}}},
      %Buffer{
        payload: <<0x45, 0xDD, "flush">>,
        pts: 0,
        metadata: %{moqx: %Unit{group_end?: true}}
      }
    ]

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers(buffers)
          })
          |> child(:framing, %Legacy{direction: :decode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_event(pipeline, :sink, %{__struct__: Membrane.MOQX.Event.MediaEnd, pts: 1_500_000})

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: "flush", pts: 1_501_000})
    refute_sink_buffer(pipeline, :sink, %Buffer{payload: <<>>})
    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "decodes the container timestamp rather than adding it to transport PTS" do
    format = %Track{
      packaging: "hang/legacy",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2},
      catalog_fields: %{"role" => "audio"}
    }

    unit = %Unit{group_end?: true, group_id: 4, object_id: 0}
    buffer = %Buffer{payload: <<0x45, 0xDC, "packet">>, pts: 999_000_000, metadata: %{moqx: unit}}

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers([buffer])
          })
          |> child(:framing, %Legacy{direction: :decode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(pipeline, :sink, %Track{packaging: "opus"})

    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: "packet",
      pts: 1_500_000,
      metadata: %{moqx: ^unit}
    })

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end

  test "encodes one microsecond timestamp prefix without changing transport PTS" do
    format = %Track{
      packaging: "opus",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    unit = %Unit{group_end?: true}
    buffer = %Buffer{payload: "packet", pts: 1_500_000, metadata: %{moqx: unit}}

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            stream_format: format,
            output: Testing.Source.output_from_buffers([buffer])
          })
          |> child(:framing, %Legacy{direction: :encode})
          |> child(:sink, Testing.Sink)
      )

    assert_sink_stream_format(pipeline, :sink, %Track{
      packaging: "hang/legacy",
      catalog_fields: %{"role" => "audio"}
    })

    # 1500 microseconds = a two-byte QUIC varint 0x45dc, independent of the encoder.
    assert_sink_buffer(pipeline, :sink, %Buffer{
      payload: <<0x45, 0xDC, "packet">>,
      pts: 1_500_000,
      metadata: %{moqx: ^unit}
    })

    assert_end_of_stream(pipeline, :sink)
    Testing.Pipeline.terminate(pipeline)
  end
end
