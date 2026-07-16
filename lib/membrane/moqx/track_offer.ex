defmodule Membrane.MOQX.TrackOffer do
  @moduledoc "A catalog-discovered track that the parent pipeline may choose to link."

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
  def from_catalog_track(catalog_track, fallback_namespace, initialization \\ nil) do
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
