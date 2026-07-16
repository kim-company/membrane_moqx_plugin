defmodule Membrane.MOQX.Track do
  @moduledoc """
  Canonical Membrane stream format exchanged by MOQX Sources and Sinks.

  Format adapters translate concrete packaged formats into and out of this
  contract. The core MOQX elements never depend on those concrete formats.
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

  @spec validate(t()) :: :ok | {:error, {:invalid_track, t()}}
  def validate(%__MODULE__{} = track) do
    if valid?(track), do: :ok, else: {:error, {:invalid_track, track}}
  end

  defp valid?(track) do
    Enum.all?([
      valid_packaging?(track.packaging),
      valid_content_types?(track.content_types),
      valid_initialization?(track.initialization),
      valid_codecs?(track.codecs),
      valid_resolution?(track.resolution),
      valid_optional_positive_integer?(track.sample_rate),
      valid_optional_positive_integer?(track.channels)
    ])
  end

  defp valid_packaging?(packaging), do: is_atom(packaging) and not is_nil(packaging)

  defp valid_content_types?(content_types),
    do: content_types != [] and Enum.all?(content_types, &(&1 in [:audio, :video]))

  defp valid_initialization?(initialization),
    do: is_binary(initialization) or is_nil(initialization)

  defp valid_codecs?(codecs), do: codecs != [] and Enum.all?(codecs, &is_binary/1)

  defp valid_resolution?(nil), do: true

  defp valid_resolution?({width, height}) do
    is_integer(width) and width >= 0 and is_integer(height) and height >= 0
  end

  defp valid_resolution?(_resolution), do: false

  defp valid_optional_positive_integer?(nil), do: true
  defp valid_optional_positive_integer?(value), do: is_integer(value) and value > 0
end
