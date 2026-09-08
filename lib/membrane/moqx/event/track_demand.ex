defmodule Membrane.MOQX.Event.TrackDemand do
  @moduledoc """
  Aggregated downstream subscription demand for one published track.

  The Sink emits this event upstream only when explicitly enabled and only on
  the transitions between no subscribers and at least one subscriber. It is a
  production optimization signal, not an authorization or graph-lifecycle
  decision.

  It is not a buffer credit, receiver acknowledgement, or backpressure
  mechanism, and does not bound network or downstream pipeline buffering.
  """

  @derive Membrane.EventProtocol
  @enforce_keys [:subscriber_count, :active?]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          subscriber_count: non_neg_integer(),
          active?: boolean()
        }
end
