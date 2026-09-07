defmodule Membrane.MOQX do
  @moduledoc """
  Membrane integration for publishing and subscribing through MOQX.

  Core Sources and Sinks exchange `Membrane.MOQX.Track` stream formats and
  buffers carrying `Membrane.MOQX.Unit` metadata. Explicit track-adapter
  filters translate concrete packaged formats at neighboring pipeline
  boundaries.

  ## Supported boundaries

  Built-in protocol selections are `:moq_lite_05`, `:draft_16`, and
  `:cloudflare_draft_14`, over native QUIC through MOQX. Selection is explicit;
  an endpoint does not negotiate a different implementation automatically.
  Draft-18 and WebTransport are not implemented by this plugin's MOQX 0.8.1
  baseline.

  * `Membrane.MOQX.Source` receives one known track with caller-supplied format.
  * `Membrane.MOQX.Sink` publishes dynamic tracks and exposes controlled
    subscription decisions and optional aggregate demand notifications.
  * `Membrane.MOQX.CatalogSource` interprets the draft-16/Cloudflare draft-14
    CMSF catalog conventions. It does not implement Lite broadcast discovery
    or HANG catalog discovery.
  * `Membrane.MOQX.Session` shares a client and routes subscription events.
  * `Membrane.MOQX.TrackAdapter.CMAF` adapts H.264/AAC CMAF metadata; encoding,
    muxing, demuxing and playback remain outside the core elements.

  Lite support currently means exact-track publication/subscription, timestamp
  conversion and demand-driven track production. No HANG catalog is generated
  or interpreted, and full Lite application interoperability is not certified.
  Carrying opaque bytes is not a promise that a media player can decode them.

  ## Delivery and verification limits

  Local acceptance, subscription readiness and EOS are not end-to-end delivery
  acknowledgements. Immediate final-buffer/EOS has lost payloads in observed
  Cloudflare draft-14 and draft-16 workflows. Aggregate subscriber demand is
  not receiver flow control or a bounded-memory delivery guarantee.

  Hermetic element tests do not certify a relay. The public Lite smoke covers
  a plugin Sink with MOQX subscribers, not a complete plugin Source/Sink
  roundtrip. Dated relay results and protocol-retirement gates live in the
  repository's `docs/relay-compatibility.md`; module documentation defines the
  API contract, while that evidence records what was actually exercised.

  Keep authorization secrets and token-bearing endpoint URLs out of logs and
  diagnostics. `MOQX.Secret` does not redact a token embedded in a URI.
  """
end
