defmodule Membrane.MOQX.PublicationUnit do
  @moduledoc """
  One atomic payload and its format-neutral publication boundary metadata.

  A track adapter produces exactly one publication unit for each incoming
  Membrane buffer. The Sink assigns MOQ coordinates without modifying the
  payload.
  """

  @enforce_keys [:payload, :segment_end?]
  defstruct @enforce_keys ++ [independent?: nil, duration: nil]

  @type t :: %__MODULE__{
          payload: binary(),
          segment_end?: boolean(),
          independent?: boolean() | nil,
          duration: Membrane.Time.t() | nil
        }
end
