defmodule Membrane.MOQX.TrackAdapterTest do
  use ExUnit.Case, async: true

  alias Membrane.Buffer
  alias Membrane.CMAF.Track, as: CMAFTrack
  alias Membrane.MOQX.{Track, TrackAdapter, Unit}

  defmodule CustomFormat do
    defstruct [:initialization]
  end

  defmodule CustomAdapter do
    @behaviour TrackAdapter

    @impl true
    def to_moqx_stream_format(%CustomFormat{initialization: initialization}, _options) do
      track = %Track{
        packaging: :custom,
        content_types: [:audio],
        initialization: initialization,
        codecs: ["custom.audio"]
      }

      {:ok, track, track}
    end

    @impl true
    def to_moqx_buffer(%Buffer{} = buffer, %Track{} = track) do
      {:ok, %{buffer | metadata: %{moqx: %Unit{segment_end?: true}}}, track}
    end
  end

  defmodule MalformedAdapter do
    @behaviour TrackAdapter

    @impl true
    def to_moqx_stream_format(stream_format, options),
      do: CustomAdapter.to_moqx_stream_format(stream_format, options)

    @impl true
    def to_moqx_buffer(_buffer, _state), do: {:ok, :not_a_buffer, nil}
  end

  defmodule MalformedTrackAdapter do
    @behaviour TrackAdapter

    @impl true
    def to_moqx_stream_format(_stream_format, _options) do
      {:ok,
       %Track{
         packaging: :custom,
         content_types: [:audio],
         initialization: nil,
         codecs: []
       }, nil}
    end

    @impl true
    def to_moqx_buffer(buffer, state), do: {:ok, buffer, state}
  end

  test "converts an H264 CMAF stream format into a canonical track" do
    stream_format = %CMAFTrack{
      content_type: :video,
      header: "cmaf-init",
      resolution: {1920, 1080},
      codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "1F"}}
    }

    assert {:ok,
            %Track{
              packaging: :cmaf,
              content_types: [:video],
              initialization: "cmaf-init",
              codecs: ["avc1.42C01F"],
              resolution: {1920, 1080}
            } = track, adapter_state} =
             TrackAdapter.to_moqx_stream_format(Membrane.MOQX.TrackAdapter.CMAF, stream_format)

    assert adapter_state == track
  end

  test "converts an AAC CMAF stream format into a canonical track" do
    stream_format = %CMAFTrack{
      content_type: :audio,
      header: "aac-init",
      codecs: %{mp4a: %{aot_id: "2", channels: 2, frequency: 48_000}}
    }

    assert {:ok,
            %Track{
              packaging: :cmaf,
              content_types: [:audio],
              initialization: "aac-init",
              codecs: ["mp4a.40.2"],
              sample_rate: 48_000,
              channels: 2
            } = track, adapter_state} =
             TrackAdapter.to_moqx_stream_format(Membrane.MOQX.TrackAdapter.CMAF, stream_format)

    assert adapter_state == track
  end

  test "converts a complete CMAF segment without changing its payload" do
    track = %Track{
      packaging: :cmaf,
      content_types: [:video],
      initialization: "init",
      codecs: ["avc1.42C01F"]
    }

    buffer = %Buffer{
      payload: "complete-cmaf-segment",
      metadata: %{duration: 2_000, independent?: true}
    }

    assert {:ok,
            %Buffer{
              payload: "complete-cmaf-segment",
              metadata: %{
                moqx: %Unit{
                  segment_end?: true,
                  independent?: true,
                  duration: 2_000
                }
              }
            }, ^track} =
             TrackAdapter.to_moqx_buffer(Membrane.MOQX.TrackAdapter.CMAF, buffer, track)
  end

  test "rejects malformed CMAF units" do
    track = %Track{
      packaging: :cmaf,
      content_types: [:video],
      initialization: "init",
      codecs: ["avc1.42C01F"]
    }

    assert {:error, :invalid_cmaf_publication_unit} =
             TrackAdapter.to_moqx_buffer(
               Membrane.MOQX.TrackAdapter.CMAF,
               %Buffer{payload: :not_binary},
               track
             )

    assert {:error, :invalid_cmaf_publication_unit} =
             TrackAdapter.to_moqx_buffer(
               Membrane.MOQX.TrackAdapter.CMAF,
               %Buffer{payload: "bytes", metadata: %{last_chunk?: :unknown}},
               track
             )
  end

  test "uses a caller-provided adapter" do
    stream_format = %CustomFormat{initialization: "custom-init"}

    assert {:ok, %Track{packaging: :custom} = track, adapter_state} =
             TrackAdapter.to_moqx_stream_format(CustomAdapter, stream_format)

    assert adapter_state == track

    assert {:ok, %Buffer{payload: "custom-media", metadata: %{moqx: %Unit{segment_end?: true}}},
            ^track} =
             TrackAdapter.to_moqx_buffer(
               CustomAdapter,
               %Buffer{payload: "custom-media"},
               track
             )
  end

  test "rejects malformed buffers returned by a custom adapter" do
    {:ok, track, adapter_state} =
      TrackAdapter.to_moqx_stream_format(
        MalformedAdapter,
        %CustomFormat{initialization: "custom-init"}
      )

    assert {:error, {:invalid_adapted_buffer, :not_a_buffer}} =
             TrackAdapter.to_moqx_buffer(
               MalformedAdapter,
               %Buffer{payload: "custom-media"},
               adapter_state
             )

    assert %Track{} = track
  end

  test "rejects a malformed canonical track returned by an adapter" do
    assert {:error, {:invalid_adapted_track, %Track{}}} =
             TrackAdapter.to_moqx_stream_format(MalformedTrackAdapter, %CustomFormat{})
  end
end
