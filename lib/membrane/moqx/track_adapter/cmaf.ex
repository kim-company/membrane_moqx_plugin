defmodule Membrane.MOQX.TrackAdapter.CMAF do
  @moduledoc "Adapts `Membrane.CMAF.Track` stream formats for MOQX publication."

  @behaviour Membrane.MOQX.TrackAdapter

  alias Membrane.MOQX.{PublicationUnit, TrackDescriptor}

  @impl true
  def describe(
        %Membrane.CMAF.Track{
          content_type: :video,
          header: header,
          resolution: resolution,
          codecs: %{
            avc1: %{profile: profile, compatibility: compatibility, level: level}
          }
        },
        _options
      )
      when is_binary(header) and is_binary(profile) and is_binary(compatibility) and
             is_binary(level) do
    {:ok,
     %TrackDescriptor{
       packaging: :cmaf,
       content_types: [:video],
       initialization: header,
       codecs: ["avc1.#{profile}#{compatibility}#{level}"],
       resolution: resolution
     }}
  end

  def describe(
        %Membrane.CMAF.Track{
          content_type: :audio,
          header: header,
          codecs: %{
            mp4a: %{aot_id: aot_id, channels: channels, frequency: sample_rate}
          }
        },
        _options
      )
      when is_binary(header) and is_binary(aot_id) and is_integer(channels) and channels > 0 and
             is_integer(sample_rate) and sample_rate > 0 do
    {:ok,
     %TrackDescriptor{
       packaging: :cmaf,
       content_types: [:audio],
       initialization: header,
       codecs: ["mp4a.40.#{aot_id}"],
       sample_rate: sample_rate,
       channels: channels
     }}
  end

  def describe(%Membrane.CMAF.Track{}, _options), do: {:error, :unsupported_cmaf_track}

  @impl true
  def publication_unit(
        %Membrane.Buffer{payload: payload, metadata: metadata},
        %TrackDescriptor{packaging: :cmaf}
      ) do
    segment_end? = Map.get(metadata, :last_chunk?, true)
    independent? = Map.get(metadata, :independent?)
    duration = Map.get(metadata, :duration)

    if is_binary(payload) and is_boolean(segment_end?) and
         (is_nil(independent?) or is_boolean(independent?)) and
         (is_nil(duration) or (is_integer(duration) and duration >= 0)) do
      {:ok,
       %PublicationUnit{
         payload: payload,
         segment_end?: segment_end?,
         independent?: independent?,
         duration: duration
       }}
    else
      {:error, :invalid_cmaf_publication_unit}
    end
  end
end
