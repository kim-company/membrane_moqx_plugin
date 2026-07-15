defmodule Membrane.MOQX.TrackDescriptor do
  @moduledoc """
  Format-neutral description of one track prepared for MOQX publication.

  Track adapters translate concrete Membrane stream formats into this stable
  contract. The Sink uses it to publish initialization data and catalog
  metadata without depending on the original stream-format structure.
  """

  @enforce_keys [:packaging, :content_types, :initialization, :codecs]
  defstruct @enforce_keys ++
              [
                resolution: nil,
                sample_rate: nil,
                channels: nil
              ]

  @type t :: %__MODULE__{
          packaging: atom(),
          content_types: [:audio | :video],
          initialization: binary() | nil,
          codecs: [binary()],
          resolution: {non_neg_integer(), non_neg_integer()} | nil,
          sample_rate: pos_integer() | nil,
          channels: pos_integer() | nil
        }
end
