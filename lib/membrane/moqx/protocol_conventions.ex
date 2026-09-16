defmodule Membrane.MOQX.ProtocolConventions do
  @moduledoc """
  Pure adaptation between explicit MOQX protocols and Membrane publication intent.

  MOQX owns protocol state and wire behavior. This module owns only deployed
  CMSF schema helpers and the options needed to express one canonical
  `Membrane.MOQX.Track` through MOQX's public API.

  Element catalog names come from the explicit MOQX application profile.
  Profile-name/encoding validation below composes MOQX's public profile names;
  catalog codecs and HANG media framing remain outside this module.
  """

  alias Membrane.MOQX.Track

  @type protocol :: :draft_18 | :moq_lite_05 | module()

  @doc "Resolves an explicit catalog profile and encoding without mislabeling payload bytes."
  @spec profile_catalog_track_name(MOQX.Profile.t(), :none | :deflate, binary() | nil) ::
          binary() | nil
  def profile_catalog_track_name(:none, :none, _override), do: nil

  def profile_catalog_track_name(profile, compression, override) do
    case MOQX.Profile.track_name(profile, compression) do
      {:ok, default} ->
        name = override || default

        if profile == :hang and
             ((compression == :deflate and name != "catalog.json.z") or
                (compression == :none and name == "catalog.json.z")) do
          raise ArgumentError, "HANG catalog name does not match its explicit compression"
        end

        name

      {:error, reason} ->
        raise ArgumentError, "unsupported catalog profile/encoding: #{inspect(reason)}"
    end
  end

  @spec draft_18?(protocol()) :: boolean()
  def draft_18?(:draft_18), do: true
  def draft_18?(MOQX.Protocol.Draft18), do: true
  def draft_18?(_protocol), do: false

  @spec moq_lite_05?(protocol()) :: boolean()
  def moq_lite_05?(:moq_lite_05), do: true
  def moq_lite_05?(MOQX.Protocol.MOQLite05), do: true
  def moq_lite_05?(_protocol), do: false

  @spec track_options(protocol(), MOQX.PublishedTrack.retention(), MOQX.publication_delivery()) ::
          keyword()
  def track_options(protocol, retention, delivery) do
    options = [retention: retention]
    if draft_18?(protocol), do: options ++ [delivery: delivery], else: options
  end

  @spec track_options(
          protocol(),
          MOQX.PublishedTrack.retention(),
          MOQX.publication_delivery(),
          keyword()
        ) :: keyword()
  def track_options(protocol, retention, delivery, lite_options) do
    if moq_lite_05?(protocol) do
      [retention: retention, delivery: delivery] ++
        Keyword.take(lite_options, [
          :timescale,
          :publisher_priority,
          :publisher_max_latency
        ])
    else
      track_options(protocol, retention, delivery)
    end
  end

  @spec catalog_priority(protocol(), 0..255) :: 0..255
  def catalog_priority(protocol, _media_priority)
      when protocol in [:draft_18, MOQX.Protocol.Draft18],
      do: 0

  def catalog_priority(_protocol, media_priority), do: media_priority

  @spec catalog(MOQX.Profile.t(), [binary()], [{binary(), binary() | nil, Track.t()}]) :: map()
  def catalog(:moqtail_cmsf, _namespace, tracks) do
    %{
      "version" => 1,
      "tracks" => Enum.map(tracks, &moqtail_track/1)
    }
  end

  def catalog(:cloudflare_cmsf, namespace, tracks) do
    %{
      "version" => 1,
      "streamingFormat" => 1,
      "streamingFormatVersion" => "0.2",
      "supportsDeltaUpdates" => false,
      "commonTrackFields" => %{"namespace" => Enum.join(namespace, "/")},
      "tracks" => Enum.map(tracks, &cloudflare_track/1)
    }
  end

  defp moqtail_track({name, _init_name, %Track{} = track}) do
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
