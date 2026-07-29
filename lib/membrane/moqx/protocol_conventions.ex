defmodule Membrane.MOQX.ProtocolConventions do
  @moduledoc """
  Pure adaptation between explicit MOQX protocols and Membrane publication intent.

  MOQX owns protocol state and wire behavior. This module owns only deployed
  catalog naming/schema choices and the options needed to express one canonical
  `Membrane.MOQX.Track` through MOQX's public API.
  """

  alias Membrane.MOQX.Track

  @type protocol :: :draft_16 | :cloudflare_draft_14 | module()

  @spec draft_16?(protocol()) :: boolean()
  def draft_16?(:draft_16), do: true
  def draft_16?(MOQX.Protocol.Draft16), do: true
  def draft_16?(_protocol), do: false

  @spec catalog_track_name(protocol(), binary() | nil) :: binary()
  def catalog_track_name(_protocol, override) when is_binary(override), do: override

  def catalog_track_name(protocol, nil),
    do: if(draft_16?(protocol), do: "catalog", else: ".catalog")

  @spec initialization_mode(protocol()) :: :inline | :separate_track
  def initialization_mode(protocol),
    do: if(draft_16?(protocol), do: :inline, else: :separate_track)

  @spec track_options(protocol(), MOQX.PublishedTrack.retention(), MOQX.publication_delivery()) ::
          keyword()
  def track_options(protocol, retention, delivery) do
    options = [retention: retention]
    if draft_16?(protocol), do: options ++ [delivery: delivery], else: options
  end

  @spec catalog_priority(protocol(), 0..255) :: 0..255
  def catalog_priority(protocol, _media_priority)
      when protocol in [:draft_16, MOQX.Protocol.Draft16],
      do: 0

  def catalog_priority(_protocol, media_priority), do: media_priority

  @spec catalog(protocol(), [binary()], [{binary(), binary() | nil, Track.t()}]) :: map()
  def catalog(protocol, namespace, tracks) do
    if draft_16?(protocol) do
      %{
        "version" => 1,
        "tracks" => Enum.map(tracks, &draft_16_track/1)
      }
    else
      %{
        "version" => 1,
        "streamingFormat" => 1,
        "streamingFormatVersion" => "0.2",
        "supportsDeltaUpdates" => false,
        "commonTrackFields" => %{"namespace" => Enum.join(namespace, "/")},
        "tracks" => Enum.map(tracks, &cloudflare_track/1)
      }
    end
  end

  defp draft_16_track({name, _init_name, %Track{} = track}) do
    track.selection_params
    |> Map.merge(track.catalog_fields)
    |> Map.put("name", name)
    |> Map.put("packaging", track.packaging)
    |> put_if_present("initData", encode_initialization(track.initialization))
  end

  defp cloudflare_track({name, init_name, %Track{} = track}) do
    track.catalog_fields
    |> Map.put("name", name)
    |> Map.put("packaging", track.packaging)
    |> put_unless_empty("selectionParams", track.selection_params)
    |> put_if_present("initTrack", init_name)
  end

  defp encode_initialization(nil), do: nil
  defp encode_initialization(initialization), do: Base.encode64(initialization)

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp put_unless_empty(map, _key, value) when value == %{}, do: map
  defp put_unless_empty(map, key, value), do: Map.put(map, key, value)
end
