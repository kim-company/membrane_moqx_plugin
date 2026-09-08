defmodule Membrane.MOQX.Hang.CMAF do
  @moduledoc """
  Explicit CMAF adapter for the HANG application profile.

  Compose with `Membrane.MOQX.TrackAdapter.ToTrack` before a HANG Sink and
  `Membrane.MOQX.TrackAdapter.FromTrack` after a HANG catalog-selected Source.
  Supported codecs are H.264 (`avc1`) video and AAC (`mp4a.40`) audio.
  Decoder dimensions, sample rate and channels use HANG's WebCodecs keys; initialization is
  carried by the catalog's CMAF container. Buffers remain complete CMAF chunks;
  no legacy timestamp prefix is added and Membrane PTS remains unchanged.
  `last_chunk?` maps to the canonical Unit group boundary in both directions.
  A muxer/demuxer remains a separate component. This adapter does not inspect
  MP4 boxes, certify random-access boundaries, or prove browser decoding.

  `Membrane.MOQX.Event.EmptyGroup` passes through the composing filters as a
  HANG codec-epoch boundary, including when the next chunk's PTS moves backward.
  This stateless adapter does not rewrite MP4 decode times or reset a demuxer
  or decoder; those downstream components own epoch handling.
  """
  @behaviour Membrane.MOQX.TrackAdapter
  alias Membrane.MOQX.TrackAdapter.CMAF

  @impl true
  def to_moqx_stream_format(%Membrane.CMAF.Track{content_type: :video} = format, options) do
    with {:ok, track, state} <- CMAF.to_moqx_stream_format(format, options) do
      params = track.selection_params

      params =
        params
        |> Map.drop(["mimeType", "width", "height"])
        |> put_present("codedWidth", params["width"])
        |> put_present("codedHeight", params["height"])

      track = %{
        track
        | selection_params: params,
          catalog_fields: Map.put(track.catalog_fields, "role", "video")
      }

      {:ok, track, state}
    end
  end

  def to_moqx_stream_format(%Membrane.CMAF.Track{content_type: :audio} = format, options) do
    with {:ok, track, state} <- CMAF.to_moqx_stream_format(format, options) do
      params = track.selection_params

      params =
        params
        |> Map.drop(["mimeType", "samplerate", "channelConfig"])
        |> put_present("sampleRate", params["samplerate"])
        |> put_present("numberOfChannels", params["channelConfig"])

      track = %{
        track
        | selection_params: params,
          catalog_fields: Map.put(track.catalog_fields, "role", "audio")
      }

      {:ok, track, state}
    end
  end

  def to_moqx_stream_format(_format, _options), do: {:error, :unsupported_hang_cmaf_track}

  @impl true
  defdelegate to_moqx_buffer(buffer, state), to: CMAF

  @impl true
  def from_moqx_stream_format(%Membrane.MOQX.Track{packaging: "cmaf"} = track, options) do
    params = track.selection_params

    params =
      params
      |> Map.drop(["codedWidth", "codedHeight", "sampleRate", "numberOfChannels"])
      |> put_present("width", params["codedWidth"])
      |> put_present("height", params["codedHeight"])
      |> put_present("samplerate", params["sampleRate"])
      |> put_present("channelConfig", params["numberOfChannels"])

    CMAF.from_moqx_stream_format(%{track | selection_params: params}, options)
  end

  def from_moqx_stream_format(_track, _options), do: {:error, :unsupported_hang_cmaf_track}

  @impl true
  defdelegate from_moqx_buffer(buffer, state), to: CMAF

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)
end
