defmodule Membrane.MOQX.TrackOffer do
  @moduledoc """
  A catalog-discovered track that the parent pipeline may choose to link.

  CMSF and HANG catalog values remain available in `catalog_track`. HANG
  offers use canonical `Track` metadata, not CMSF coercion: decoder configuration
  is in selection parameters, initialization bytes in `initialization`, and
  container/role metadata in catalog fields. `hang/legacy` and `hang/loc` denote
  framed media, not raw codec packets. Recognized metadata does not certify
  an adapter or decoder; unknown media is reported as an unsupported offer.
  """

  alias Membrane.MOQX.Track

  @enforce_keys [:track_ref, :stream_format, :catalog_track]
  defstruct @enforce_keys ++ [advertised?: true]

  @type t :: %__MODULE__{
          track_ref: MOQX.TrackRef.t(),
          stream_format: Track.t(),
          catalog_track: MOQX.Catalog.Track.t(),
          advertised?: boolean()
        }

  @spec from_catalog_track(MOQX.Catalog.Track.t(), [binary()], binary() | nil) ::
          {:ok, t()} | {:error, term()}
  def from_catalog_track(catalog_track, fallback_namespace, initialization \\ nil)

  def from_catalog_track(
        %{decoder: %MOQX.Catalog.Decoder{}, container: container} = media,
        fallback_namespace,
        _initialization
      ) do
    with true <- media.metadata_status == :recognized,
         %MOQX.TrackRef{} = ref <- MOQX.Catalog.Track.track_ref(media, fallback_namespace) do
      decoder_keys =
        ~w(codec codedWidth codedHeight displayAspectWidth displayAspectHeight sampleRate numberOfChannels optimizeForLatency)

      format = %Track{
        packaging: if(container.kind == "cmaf", do: "cmaf", else: "hang/" <> container.kind),
        initialization:
          if(container.kind == "cmaf", do: container.init, else: media.decoder.description),
        selection_params: Map.take(media.raw, decoder_keys),
        catalog_fields:
          media.raw
          |> Map.drop(["description", "broadcast" | decoder_keys])
          |> Map.put("role", media.role)
      }

      with :ok <- Track.validate(format),
           do: {:ok, %__MODULE__{track_ref: ref, stream_format: format, catalog_track: media}}
    else
      false -> {:error, {:unsupported_media, media.metadata_status}}
      {:error, _reason} = error -> error
    end
  end

  def from_catalog_track(catalog_track, fallback_namespace, initialization) do
    raw = catalog_track.raw
    packaging = catalog_track.packaging

    track = %Track{
      packaging: packaging,
      initialization: initialization || catalog_track.init_data,
      selection_params: Map.get(raw, "selectionParams", %{}),
      catalog_fields:
        Map.drop(raw, [
          "name",
          "namespace",
          "initTrack",
          "initData",
          "packaging",
          "selectionParams"
        ])
    }

    with true <- is_binary(packaging) and packaging != "",
         :ok <- Track.validate(track) do
      track_ref =
        case catalog_track.namespace do
          namespace when is_binary(namespace) ->
            MOQX.Catalog.Track.track_ref(catalog_track)

          _none ->
            %MOQX.TrackRef{namespace: fallback_namespace, track: catalog_track.name}
        end

      {:ok,
       %__MODULE__{
         track_ref: track_ref,
         stream_format: track,
         catalog_track: catalog_track
       }}
    else
      false -> {:error, :catalog_track_missing_packaging}
      {:error, _reason} = error -> error
    end
  end
end
