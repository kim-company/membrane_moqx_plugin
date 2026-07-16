defmodule Membrane.MOQX.TrackAdapter.CMAF do
  @moduledoc "Adapts `Membrane.CMAF.Track` to and from the canonical MOQX pad contract."

  @behaviour Membrane.MOQX.TrackAdapter

  alias Membrane.MOQX.{Track, Unit}

  @impl true
  def to_moqx_stream_format(
        %Membrane.CMAF.Track{
          content_type: :video,
          header: header,
          resolution: resolution,
          codecs: %{avc1: %{profile: profile, compatibility: compatibility, level: level}}
        },
        _options
      )
      when is_binary(header) and is_binary(profile) and is_binary(compatibility) and
             is_binary(level) do
    track = %Track{
      packaging: :cmaf,
      content_types: [:video],
      initialization: header,
      codecs: ["avc1.#{profile}#{compatibility}#{level}"],
      resolution: resolution
    }

    {:ok, track, track}
  end

  def to_moqx_stream_format(
        %Membrane.CMAF.Track{
          content_type: :audio,
          header: header,
          codecs: %{mp4a: %{aot_id: aot_id, channels: channels, frequency: sample_rate}}
        },
        _options
      )
      when is_binary(header) and is_binary(aot_id) and is_integer(channels) and channels > 0 and
             is_integer(sample_rate) and sample_rate > 0 do
    track = %Track{
      packaging: :cmaf,
      content_types: [:audio],
      initialization: header,
      codecs: ["mp4a.40.#{aot_id}"],
      sample_rate: sample_rate,
      channels: channels
    }

    {:ok, track, track}
  end

  def to_moqx_stream_format(%Membrane.CMAF.Track{}, _options),
    do: {:error, :unsupported_cmaf_track}

  @impl true
  def to_moqx_buffer(
        %Membrane.Buffer{payload: payload, metadata: metadata} = buffer,
        %Track{packaging: :cmaf} = track
      ) do
    unit = %Unit{
      segment_end?: Map.get(metadata, :last_chunk?, true),
      independent?: Map.get(metadata, :independent?),
      duration: Map.get(metadata, :duration)
    }

    with true <- is_binary(payload),
         {:ok, unit} <- Unit.validate(unit) do
      {:ok, %{buffer | metadata: %{moqx: unit}}, track}
    else
      _invalid -> {:error, :invalid_cmaf_publication_unit}
    end
  end

  @impl true
  def from_moqx_stream_format(
        %Track{
          packaging: :cmaf,
          content_types: [:video],
          initialization: initialization,
          codecs: [
            <<"avc1.", profile::binary-size(2), compatibility::binary-size(2),
              level::binary-size(2)>>
          ],
          resolution: resolution
        } = track,
        _options
      )
      when is_binary(initialization) do
    stream_format = %Membrane.CMAF.Track{
      content_type: :video,
      header: initialization,
      resolution: resolution,
      codecs: %{avc1: %{profile: profile, compatibility: compatibility, level: level}}
    }

    {:ok, stream_format, track}
  end

  def from_moqx_stream_format(
        %Track{
          packaging: :cmaf,
          content_types: [:audio],
          initialization: initialization,
          codecs: [<<"mp4a.40.", aot_id::binary>>],
          sample_rate: sample_rate,
          channels: channels
        } = track,
        _options
      )
      when is_binary(initialization) and is_integer(sample_rate) and sample_rate > 0 and
             is_integer(channels) and channels > 0 and byte_size(aot_id) > 0 do
    stream_format = %Membrane.CMAF.Track{
      content_type: :audio,
      header: initialization,
      codecs: %{mp4a: %{aot_id: aot_id, channels: channels, frequency: sample_rate}}
    }

    {:ok, stream_format, track}
  end

  def from_moqx_stream_format(%Track{}, _options), do: {:error, :unsupported_moqx_cmaf_track}

  @impl true
  def from_moqx_buffer(%Membrane.Buffer{} = buffer, %Track{packaging: :cmaf} = track) do
    with {:ok, %Unit{} = unit} <- Unit.from_buffer(buffer) do
      metadata =
        %{moqx: unit, last_chunk?: unit.segment_end?}
        |> put_if_present(:independent?, unit.independent?)
        |> put_if_present(:duration, unit.duration)

      {:ok, %{buffer | metadata: metadata}, track}
    end
  end

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)
end
