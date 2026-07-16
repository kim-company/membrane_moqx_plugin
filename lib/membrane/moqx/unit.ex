defmodule Membrane.MOQX.Unit do
  @moduledoc """
  Canonical metadata for one atomic MOQX object buffer.

  Publication adapters declare whether the object ends its MOQ group. A Source
  additionally fills received coordinates, priority, and status when available.
  """

  @enforce_keys [:group_end?]
  defstruct @enforce_keys ++
              [
                group_id: nil,
                subgroup_id: nil,
                object_id: nil,
                publisher_priority: nil,
                status: nil
              ]

  @type status :: :object_does_not_exist | :end_of_group | :end_of_track | nil

  @type t :: %__MODULE__{
          group_end?: boolean(),
          group_id: non_neg_integer() | nil,
          subgroup_id: non_neg_integer() | nil,
          object_id: non_neg_integer() | nil,
          publisher_priority: 0..255 | nil,
          status: status()
        }

  @spec from_buffer(Membrane.Buffer.t()) :: {:ok, t()} | {:error, term()}
  def from_buffer(%Membrane.Buffer{payload: payload, metadata: metadata}) do
    case metadata do
      %{moqx: %__MODULE__{} = unit} when is_binary(payload) -> validate(unit)
      %{moqx: value} -> {:error, {:invalid_moqx_unit, value}}
      _metadata -> {:error, :missing_moqx_unit}
    end
  end

  @spec validate(t()) :: {:ok, t()} | {:error, {:invalid_moqx_unit, t()}}
  def validate(%__MODULE__{} = unit) do
    valid? =
      is_boolean(unit.group_end?) and
        optional_non_negative_integer?(unit.group_id) and
        optional_non_negative_integer?(unit.subgroup_id) and
        optional_non_negative_integer?(unit.object_id) and
        optional_priority?(unit.publisher_priority) and
        unit.status in [:object_does_not_exist, :end_of_group, :end_of_track, nil]

    if valid?, do: {:ok, unit}, else: {:error, {:invalid_moqx_unit, unit}}
  end

  defp optional_non_negative_integer?(value),
    do: is_nil(value) or (is_integer(value) and value >= 0)

  defp optional_priority?(value), do: is_nil(value) or (is_integer(value) and value in 0..255)
end
