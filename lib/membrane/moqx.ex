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
  Draft-18 and WebTransport are not implemented by this plugin's MOQX 0.9.0
  baseline.

  * `Membrane.MOQX.Source` receives one known track with caller-supplied format.
  * `Membrane.MOQX.Sink` publishes dynamic tracks and exposes controlled
    subscription decisions and optional aggregate demand notifications.
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

  Full Lite/HANG support is incomplete: the pinned relay cannot request an
  absent track through controlled admission alone (MOQX #47), and MOQX 0.9.0
  cannot publish the empty groups needed for HANG codec-epoch discontinuities
  (MOQX #48). Register tracks before relay admission. Do not use this version
  for discontinuous codec epochs or interpret an empty-payload media endpoint
  as an empty-group discontinuity.

  ## Pinned capability matrix

  This matrix uses MOQX 0.9.0 and moq-dev/moq commit
  `fd477082c43c3c0738fb62d077d85ea078f10045`. Test names below refer to repository
  ExUnit modules; the dated evidence report contains native/browser runs.

  | Capability | Contract and verification | Status |
  | --- | --- | --- |
  | Raw Lite exact tracks, PTS and final-buffer/EOS | Source/Sink; SourceTest, SinkTest, LiteRoundtripTest; local/public QUIC | Implemented, verified for exercised paths |
  | Broadcast snapshot/add/remove/cancel and owner isolation | Session; SessionTest; raw native roundtrip discovers publication | Implemented, tested |
  | HANG catalog offers, selection, malformed snapshots and removal | CatalogSource/TrackOffer; CatalogSourceTest and LiteRoundtripTest | Implemented; complete failure/reuse matrix pending |
  | Empty/late HANG catalog publication | Sink; SinkTest and native HANG roundtrip | Implemented, tested |
  | Legacy Opus/H.264 framing, PTS, keyframe flags and MediaEnd | Hang.Legacy; HangLegacyTest; local/public reference-player decoding | Implemented; reverse legacy reference proof pending |
  | H.264/AAC CMAF metadata and chunks | Hang.CMAF; HangCMAFTest and SinkTest; both reference directions local/public | Implemented, verified for pinned stack |
  | Multi-subscriber demand, rejection and teardown | SinkTest exercises individual transport peers | Full Lite Source/Sink lifecycle matrix pending |
  | Hermetic full Sink-to-Source semantic relay | Native roundtrips exist; element fixtures are separate | Not yet implemented |
  | Absent-track provisioning through pinned relay | Register metadata before controlled admission; MOQX #47 | Upstream-blocked |
  | Empty-group HANG codec-epoch discontinuity | No publication operation or Source event; MOQX #48 | Upstream-blocked, unsupported |
  | LOC, other codecs and universal browser playback | Opaque transport/recognized catalog metadata is not decoder support | Unsupported by supplied adapters |

  ## Delivery and verification limits

  Local acceptance, subscription readiness and EOS are not end-to-end delivery
  acknowledgements. Immediate final-buffer/EOS has lost payloads in observed
  Cloudflare draft-14 and draft-16 workflows. Aggregate subscriber demand is
  not receiver flow control or a bounded-memory delivery guarantee.

  Verification includes complete raw and catalog-selected HANG plugin
  Source/Sink roundtrips through local and public Lite relays, reference-browser
  Opus/H.264 and H.264/AAC CMAF decoding, and reverse reference CMAF publication
  received by the plugin and independently decoded. This is a pinned test
  matrix, not universal codec/browser or lifecycle certification. See
  `docs/hang-interop-2026-09-07.md` for exact versions and remaining gates, and
  `docs/relay-compatibility.md` for older results and protocol-retirement gates.

  Keep authorization secrets and token-bearing endpoint URLs out of logs and
  diagnostics. `MOQX.Secret` does not redact a token embedded in a URI.
  """
end
