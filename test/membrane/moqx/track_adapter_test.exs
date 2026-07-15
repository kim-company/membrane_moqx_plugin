defmodule Membrane.MOQX.TrackAdapterTest do
  use ExUnit.Case, async: true

  alias Membrane.Buffer
  alias Membrane.CMAF.Track
  alias Membrane.MOQX.{PublicationUnit, TrackAdapter, TrackDescriptor}

  defmodule CustomFormat do
    defstruct [:initialization]
  end

  defmodule CustomAdapter do
    @behaviour TrackAdapter

    @impl true
    def describe(%CustomFormat{initialization: initialization}, _options) do
      {:ok,
       %TrackDescriptor{
         packaging: :custom,
         content_types: [:audio],
         initialization: initialization,
         codecs: ["custom.audio"]
       }}
    end

    @impl true
    def publication_unit(%Buffer{payload: payload}, _descriptor) do
      {:ok, %PublicationUnit{payload: payload, segment_end?: true}}
    end
  end

  defmodule MalformedAdapter do
    @behaviour TrackAdapter

    @impl true
    def describe(stream_format, options), do: CustomAdapter.describe(stream_format, options)

    @impl true
    def publication_unit(_buffer, _descriptor), do: {:ok, :not_a_publication_unit}
  end

  defmodule MalformedDescriptorAdapter do
    @behaviour TrackAdapter

    @impl true
    def describe(_stream_format, _options) do
      {:ok,
       %TrackDescriptor{
         packaging: "not-an-atom",
         content_types: [:audio],
         initialization: nil,
         codecs: []
       }}
    end

    @impl true
    def publication_unit(buffer, descriptor),
      do: CustomAdapter.publication_unit(buffer, descriptor)
  end

  test "describes an H264 CMAF track through the built-in adapter" do
    stream_format = %Track{
      content_type: :video,
      header: "cmaf-init",
      resolution: {1920, 1080},
      codecs: %{
        avc1: %{profile: "42", compatibility: "C0", level: "1F"}
      }
    }

    assert {:ok, Membrane.MOQX.TrackAdapter.CMAF,
            %TrackDescriptor{
              packaging: :cmaf,
              content_types: [:video],
              initialization: "cmaf-init",
              codecs: ["avc1.42C01F"],
              resolution: {1920, 1080},
              sample_rate: nil,
              channels: nil
            }} = TrackAdapter.describe(stream_format)
  end

  test "describes an AAC CMAF track through the built-in adapter" do
    stream_format = %Track{
      content_type: :audio,
      header: "aac-init",
      codecs: %{
        mp4a: %{aot_id: "2", channels: 2, frequency: 48_000}
      }
    }

    assert {:ok, Membrane.MOQX.TrackAdapter.CMAF,
            %TrackDescriptor{
              packaging: :cmaf,
              content_types: [:audio],
              initialization: "aac-init",
              codecs: ["mp4a.40.2"],
              resolution: nil,
              sample_rate: 48_000,
              channels: 2
            }} = TrackAdapter.describe(stream_format)
  end

  test "normalizes a complete CMAF segment without changing its payload" do
    descriptor = %TrackDescriptor{
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
            %PublicationUnit{
              payload: "complete-cmaf-segment",
              segment_end?: true,
              independent?: true,
              duration: 2_000
            }} =
             TrackAdapter.publication_unit(
               Membrane.MOQX.TrackAdapter.CMAF,
               buffer,
               descriptor
             )
  end

  test "rejects malformed CMAF publication units" do
    descriptor = %TrackDescriptor{
      packaging: :cmaf,
      content_types: [:video],
      initialization: "init",
      codecs: ["avc1.42C01F"]
    }

    assert {:error, :invalid_cmaf_publication_unit} =
             TrackAdapter.publication_unit(
               Membrane.MOQX.TrackAdapter.CMAF,
               %Buffer{payload: :not_binary},
               descriptor
             )

    assert {:error, :invalid_cmaf_publication_unit} =
             TrackAdapter.publication_unit(
               Membrane.MOQX.TrackAdapter.CMAF,
               %Buffer{payload: "bytes", metadata: %{last_chunk?: :unknown}},
               descriptor
             )
  end

  test "uses an explicitly selected custom adapter" do
    stream_format = %CustomFormat{initialization: "custom-init"}

    assert {:ok, CustomAdapter,
            %TrackDescriptor{
              packaging: :custom,
              content_types: [:audio],
              initialization: "custom-init",
              codecs: ["custom.audio"]
            } = descriptor} = TrackAdapter.describe(stream_format, adapter: CustomAdapter)

    assert {:ok, %PublicationUnit{payload: "custom-media", segment_end?: true}} =
             TrackAdapter.publication_unit(
               CustomAdapter,
               %Buffer{payload: "custom-media"},
               descriptor
             )
  end

  test "rejects malformed publication units returned by a custom adapter" do
    {:ok, MalformedAdapter, descriptor} =
      TrackAdapter.describe(%CustomFormat{initialization: "custom-init"},
        adapter: MalformedAdapter
      )

    assert {:error, {:invalid_publication_unit, :not_a_publication_unit}} =
             TrackAdapter.publication_unit(
               MalformedAdapter,
               %Buffer{payload: "custom-media"},
               descriptor
             )
  end

  test "rejects a malformed descriptor returned by a custom adapter" do
    assert {:error, {:invalid_track_descriptor, %TrackDescriptor{}}} =
             TrackAdapter.describe(%CustomFormat{}, adapter: MalformedDescriptorAdapter)
  end
end
