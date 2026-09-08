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
  Draft-18 and WebTransport are not implemented by this plugin's MOQX 0.10.0
  baseline.

  * `Membrane.MOQX.Source` receives one known track with caller-supplied format.
  * `Membrane.MOQX.Sink` publishes dynamic tracks and exposes controlled
    metadata provisioning, subscription decisions, and aggregate demand notifications.
  * `Membrane.MOQX.CatalogSource` interprets explicitly selected HANG or CMSF
    catalog profiles and offers tracks for pipeline-controlled attachment.
  * `Membrane.MOQX.Session` shares a client and routes subscription and Lite
    broadcast-discovery events to their individual owners.
  * `Membrane.MOQX.TrackAdapter.CMAF` adapts H.264/AAC CMAF metadata; encoding,
    muxing, demuxing and playback remain outside the core elements.

  Transport and application profile are independent selections. The default
  `:none` profile keeps exact tracks opaque and catalog-free. Select `:hang`
  explicitly on Lite CatalogSource/Sink and compose `Membrane.MOQX.Hang.Legacy`
  for Opus/H.264 or `Membrane.MOQX.Hang.CMAF` for H.264/AAC CMAF. These adapters
  do not encode, decode, pace, or reorder playback. LOC is not implemented.

  Lite metadata provisioning and subscription authorization are separate
  parent-owned decisions. `Sink.missing_track_metadata: :controlled` permits
  creating a requested track by adding its pad/format before admission. A
  genuine `Event.EmptyGroup` starts a new codec epoch without a payload or
  timestamp; HANG adapters preserve that boundary, and the decoder owner
  applies the reset. It is not a zero-byte object, MediaEnd, or EOS.

  ## Pinned capability matrix

  This matrix uses published MOQX 0.10.0, Lite `draft-lcurley-moq-lite-05`,
  and moq-dev/moq commit
  `fd477082c43c3c0738fb62d077d85ea078f10045`. Test names below refer to repository
  ExUnit modules; the dated evidence report contains native/browser runs.

  | Capability | Contract and verification | Status |
  | --- | --- | --- |
  | Raw Lite exact tracks, PTS and final-buffer/EOS | Source/Sink; SourceTest, SinkTest, LiteRoundtripTest; local/public QUIC | Implemented, verified for exercised paths |
  | Broadcast snapshot/add/remove/cancel and owner isolation | Session; SessionTest; native HANG roundtrip distinguishes track withdrawal from broadcast departure on local/public relays | Implemented, tested |
  | HANG catalog offers, selection, malformed snapshots and removal | CatalogSource/TrackOffer; CatalogSourceTest and LiteRoundtripTest: retained offers through malformed snapshots, deselection/reselection, rejection and parent-managed replacement | Selected Sources are isolated; catalog and sibling selections survive. Parent handles removed output pads/downstream branches; no automatic retry |
  | Plain/DEFLATE HANG catalog publication and retrieval | Sink/CatalogSource `catalog_compression`; HangCatalogLifecycleTest, CompressedCatalogTest: concurrent/late readers, configuration changes, usable/unsupported coexistence, actual DEFLATE bytes, removal | Implemented, tested; one encoding per publication, no fallback or dual-format publication |
  | Legacy Opus/H.264 framing, PTS, keyframe flags and MediaEnd | Hang.Legacy; HangLegacyTest; both reference directions local/public, including independent decoded output | Implemented, verified for pinned stack; reverse decoder orders groups offline, not a plugin playback guarantee |
  | H.264/AAC CMAF metadata and chunks | Hang.CMAF; HangCMAFTest and SinkTest; both reference directions local/public | Implemented, verified for pinned stack |
  | Multi-subscriber demand and owner teardown | LiteRoundtripTest and native LiteMultiSourceTest: shared Sources, abrupt departure, survivor media, zero demand and resubscription; SinkTest: controlled decisions | Exercised paths verified hermetically and on local/public relays |
  | Hermetic full Sink-to-Source semantic relay | LiteRoundtripTest: raw groups/PTS/immediate EOS and HANG catalog selection/media/offer withdrawal | Implemented, tested |
  | Absent-track provisioning before independent admission | Sink; MetadataProvisioningTest, native LiteMetadataProvisioningTest: dynamic registration, rejection, timeout, exact media/EOS, teardown | Implemented with published MOQX API; local/public native proof |
  | Empty-group HANG codec-epoch discontinuity | Source/Sink Event.EmptyGroup; EmptyGroupSourceTest, EmptyGroupRoundtripTest, HangLegacyTest, HangCMAFTest | Implemented; explicit decoder reset/ordering ownership remains downstream |
  | Runtime subscription updates | Session.update_subscription, Source parent command, CatalogSource pad command; SubscriptionUpdateTest checks ownership, stale/invalid decisions, wire options and nonterminal peer rejection | Implemented; local send admission is not a peer acknowledgement |
  | Lite immutable track metadata, priority/order/latency and group ranges | TrackInfo reception, Sink pad options and subscription/update options through MOQX | Supported options are version-specific; invalid options retain upstream errors |
  | Other Lite surfaces: FETCH, bandwidth PROBE, GOAWAY and datagram media | Not exposed by MOQX 0.10.0's Lite operations/delivery capabilities | Unsupported here; no emulation or silent fallback |
  | Alternative bindings: WebTransport, Qmux/TCP/TLS and WebSocket | Plugin connections use native QUIC; browser proof uses a reference peer through a relay | Unsupported plugin transports; browser compatibility does not imply browser-native plugin transport |
  | LOC, other codecs and universal browser playback | Opaque transport/recognized catalog metadata is not decoder support | Unsupported by supplied adapters |

  This is a native publication/subscription/discovery integration and explicit
  media-profile matrix, not implementation of every optional Lite wire surface.
  HANG compressed catalogs use `catalog.json.z`; custom compressed names are
  unsupported because MOQX selects subscription decoding by that conventional
  name. CMSF compression is not supported. Raw exact-track payloads remain opaque.

  ## Delivery and verification limits

  Local acceptance, subscription readiness and EOS are not end-to-end delivery
  acknowledgements. Immediate final-buffer/EOS has lost payloads in observed
  Cloudflare draft-14 and draft-16 workflows. Aggregate subscriber demand is
  not receiver flow control or a bounded-memory delivery guarantee.

  Verification includes complete raw and catalog-selected HANG plugin
  Source/Sink roundtrips through local and public Lite relays, reference-browser
  Opus/H.264 and H.264/AAC CMAF decoding, and reverse reference legacy/CMAF
  publication received by the plugin and independently decoded. Reverse legacy
  capture preserves arrival order; its offline decoder orders groups/objects
  explicitly. This is not bounded-latency playback or A/V synchronization proof.
  Selected Sources have individual crash groups; parents handle removed output
  pads and isolate affected downstream branches for outer pipeline survival.
  Catalog/session failures remain Bin-wide. This is a pinned test
  matrix, not universal codec/browser or lifecycle certification. See
  `docs/hang-interop-2026-09-07.md` for exact versions and remaining gates, and
  `docs/relay-compatibility.md` for older results and protocol-retirement gates.

  Keep authorization secrets and token-bearing endpoint URLs out of logs and
  diagnostics. `MOQX.Secret` does not redact a token embedded in a URI.
  `DiagnosticsTest` verifies wrapped-authorization redaction during a real
  Sink connection failure; this is not a blanket guarantee for arbitrary
  caller-supplied URLs or peer error text.
  """
end
