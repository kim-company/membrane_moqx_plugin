defmodule Membrane.MOQX.Event.EmptyGroup do
  @moduledoc """
  A complete MoQ Lite group containing zero objects.

  Source preserves the received `group_id`; producers may leave it `nil`.
  Sink allocates its own next outgoing group ID. This event has no timestamp
  and is distinct from a zero-byte object, `Membrane.MOQX.Event.MediaEnd`,
  and end of stream.

  The event is profile-neutral. HANG interprets it as a codec-epoch boundary:
  downstream demuxers and decoders must reset their epoch state as appropriate.
  It does not itself reset a decoder, rewrite timestamps, or reorder concurrent
  incoming group streams.
  """

  @derive Membrane.EventProtocol
  defstruct group_id: nil

  @type t :: %__MODULE__{group_id: non_neg_integer() | nil}
end
