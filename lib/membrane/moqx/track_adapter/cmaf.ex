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
        options
      )
      when is_binary(header) and is_binary(profile) and is_binary(compatibility) and
             is_binary(level) do
    track = %Track{
      packaging: "cmaf",
      initialization: header,
      selection_params:
        %{
          "codec" => "avc1.#{profile}#{compatibility}#{level}",
          "mimeType" => "video/mp4"
        }
        |> put_resolution(resolution)
    }

    track = put_adapter_metadata(track, options)
    {:ok, track, track}
  end

  def to_moqx_stream_format(
        %Membrane.CMAF.Track{
          content_type: :audio,
          header: header,
          codecs: %{mp4a: %{aot_id: aot_id, channels: channels, frequency: sample_rate}}
        },
        options
      )
      when is_binary(header) and is_binary(aot_id) and is_integer(channels) and channels > 0 and
             is_integer(sample_rate) and sample_rate > 0 do
    track = %Track{
      packaging: "cmaf",
      initialization: header,
      selection_params: %{
        "codec" => "mp4a.40.#{aot_id}",
        "mimeType" => "audio/mp4",
        "samplerate" => sample_rate,
        "channelConfig" => channels
      }
    }

    track = put_adapter_metadata(track, options)
    {:ok, track, track}
  end

  def to_moqx_stream_format(%Membrane.CMAF.Track{}, _options),
    do: {:error, :unsupported_cmaf_track}

  @impl true
  def to_moqx_buffer(
        %Membrane.Buffer{payload: payload, metadata: metadata} = buffer,
        %Track{packaging: "cmaf"} = track
      ) do
    unit = %Unit{
      group_end?: Map.get(metadata, :last_chunk?, true)
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
          packaging: "cmaf",
          initialization: initialization,
          selection_params:
            %{
              "codec" =>
                <<"avc1.", profile::binary-size(2), compatibility::binary-size(2),
                  level::binary-size(2)>>
            } = selection_params
        } = track,
        _options
      )
      when is_binary(initialization) do
    stream_format = %Membrane.CMAF.Track{
      content_type: :video,
      header: initialization,
      resolution: resolution(selection_params),
      codecs: %{avc1: %{profile: profile, compatibility: compatibility, level: level}}
    }

    {:ok, stream_format, track}
  end

  def from_moqx_stream_format(
        %Track{
          packaging: "cmaf",
          initialization: initialization,
          selection_params: %{
            "codec" => <<"mp4a.40.", aot_id::binary>>,
            "samplerate" => sample_rate,
            "channelConfig" => channels
          }
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
  def from_moqx_buffer(%Membrane.Buffer{} = buffer, %Track{packaging: "cmaf"} = track) do
    with {:ok, %Unit{} = unit} <- Unit.from_buffer(buffer) do
      metadata = %{moqx: unit, last_chunk?: unit.group_end?}

      {:ok, %{buffer | metadata: metadata}, track}
    end
  end

  defp put_resolution(params, nil), do: params

  defp put_resolution(params, {width, height}) do
    params
    |> Map.put("width", width)
    |> Map.put("height", height)
  end

  defp resolution(%{"width" => width, "height" => height}), do: {width, height}
  defp resolution(_selection_params), do: nil

  defp put_adapter_metadata(track, options) do
    selection_params = Keyword.get(options, :selection_params, %{})
    catalog_fields = Keyword.get(options, :catalog_fields, %{})

    %{
      track
      | selection_params: Map.merge(track.selection_params, selection_params),
        catalog_fields: catalog_fields
    }
  end
end
