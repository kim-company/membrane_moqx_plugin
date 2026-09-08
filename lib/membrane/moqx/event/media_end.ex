defmodule Membrane.MOQX.Event.MediaEnd do
  @moduledoc """
  Exclusive endpoint of source media, in Membrane nanoseconds.

  HANG legacy decoding emits this event instead of passing an empty codec
  payload to a decoder. It is not EOS: terminal codec packets may follow.
  The downstream decoder must process those packets and discard decoded samples
  at or after `pts`. This encoded-media plugin cannot trim decoded samples.
  """
  @derive Membrane.EventProtocol
  @enforce_keys [:pts, :unit]
  defstruct @enforce_keys
  @type t :: %__MODULE__{pts: non_neg_integer(), unit: Membrane.MOQX.Unit.t()}
end
