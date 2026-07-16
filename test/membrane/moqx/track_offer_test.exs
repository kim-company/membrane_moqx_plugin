defmodule Membrane.MOQX.TrackOfferTest do
  use ExUnit.Case, async: true

  alias Membrane.MOQX.{Track, TrackOffer}

  test "converts an open-ended catalog entry into the canonical pad format" do
    catalog_track = %MOQX.Catalog.Track{
      name: "captions",
      packaging: "webvtt",
      raw: %{
        "name" => "captions",
        "packaging" => "webvtt",
        "selectionParams" => %{"mimeType" => "text/vtt", "lang" => "it"},
        "schema" => "urn:example:captions"
      }
    }

    assert {:ok,
            %TrackOffer{
              track_ref: %MOQX.TrackRef{
                namespace: ["live", "offers"],
                track: "captions"
              },
              stream_format: %Track{
                packaging: "webvtt",
                initialization: nil,
                selection_params: %{"mimeType" => "text/vtt", "lang" => "it"},
                catalog_fields: %{"schema" => "urn:example:captions"}
              }
            }} = TrackOffer.from_catalog_track(catalog_track, ["live", "offers"])
  end
end
