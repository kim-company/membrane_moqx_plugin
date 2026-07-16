defmodule Membrane.MOQX.Track do
  @moduledoc """
  Canonical Membrane stream format exchanged by MOQX Sources and Sinks.

  `packaging` is an open application-defined identifier. Selection parameters
  and additional catalog fields are JSON-compatible string-keyed maps, so the
  contract can describe media, subtitles, timed metadata, or arbitrary data.
  Core MOQX elements never interpret format-specific values.

  The Sink owns catalog structure and rejects `catalog_fields` entries that
  would overwrite `name`, namespace, initialization, packaging, or selection
  parameters.
  """

  @reserved_catalog_fields ~w(name namespace initTrack initData packaging selectionParams)

  @enforce_keys [:packaging, :initialization]
  defstruct @enforce_keys ++
              [
                selection_params: %{},
                catalog_fields: %{}
              ]

  @type t :: %__MODULE__{
          packaging: binary(),
          initialization: binary() | nil,
          selection_params: map(),
          catalog_fields: map()
        }

  @spec validate(t()) :: :ok | {:error, {:invalid_track, t()}}
  def validate(%__MODULE__{} = track) do
    if valid?(track), do: :ok, else: {:error, {:invalid_track, track}}
  end

  defp valid?(track) do
    is_binary(track.packaging) and byte_size(track.packaging) > 0 and
      valid_initialization?(track.initialization) and
      valid_json_map?(track.selection_params) and valid_catalog_fields?(track.catalog_fields)
  end

  defp valid_initialization?(initialization),
    do: is_binary(initialization) or is_nil(initialization)

  defp valid_catalog_fields?(fields) do
    valid_json_map?(fields) and
      Enum.all?(Map.keys(fields), &(&1 not in @reserved_catalog_fields))
  end

  defp valid_json_map?(value) when is_map(value) do
    Enum.all?(value, fn {key, value} -> is_binary(key) and valid_json_value?(value) end)
  end

  defp valid_json_map?(_value), do: false

  defp valid_json_value?(value)
       when is_binary(value) or is_boolean(value) or is_number(value) or is_nil(value),
       do: true

  defp valid_json_value?(value) when is_list(value), do: Enum.all?(value, &valid_json_value?/1)
  defp valid_json_value?(value) when is_map(value), do: valid_json_map?(value)
  defp valid_json_value?(_value), do: false
end
