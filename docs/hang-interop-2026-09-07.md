# HANG interoperability evidence — 2026-09-07

These results apply to the current MOQX 0.9.0 integration work, not to a
published plugin release. They supersede the older "no HANG implementation"
baseline only for the combinations explicitly exercised below.

## Pins and environment

- MOQX 0.9.0 (Hex, project lockfile).
- Reference relay and player source: moq-dev/moq
  `fd477082c43c3c0738fb62d077d85ea078f10045`.
- Reference player `@moq/watch` 0.5.2, frozen Bun lockfile, Bun 1.3.14.
- Browser: Google Chrome 151.0.7922.174, isolated headless profile on macOS.
- Synthetic fixture generation: FFmpeg 8.1.1, libx264 baseline H.264,
  160×90 at 10 fps; libopus stereo, 48 kHz, 440 Hz signal.
- Local relay: native QUIC/WebTransport listener, explicitly `moq-lite-05`;
  ECDSA P-256 certificate pinned in the browser, native TLS peer verification.
- Public relay: native `moql://cdn.moq.dev:443/anon` and browser
  `https://cdn.moq.dev/anon`, publicly trusted TLS. Its deployed binary revision
  is unknown; the player source is pinned, not the public deployment.

## Verified pipeline behavior

`test/membrane/moqx/lite_roundtrip_test.exs` provides hermetic full-pipeline
coverage through `MOQX.Testing.Transport`, not fabricated receiver events:

- Raw Sink → semantic relay → Source: exact payload, PTS, group/object
  coordinates and boundaries across two groups, with final media before EOS.
- HANG Sink → semantic relay → CatalogSource → selected Source → Legacy:
  exact Opus packet/PTS, final media before EOS and catalog offer withdrawal.
- Two Sources sharing a Session: both receive media, abrupt first-owner death
  leaves the second receiving, final-owner death returns aggregate demand to
  zero, and a new Source can subscribe and receive new media on that Session.

The scoped relay translates subscription IDs and forwards actual requests,
responses, group streams, subscription updates and FIN/RESET. It does not
generate media or add a delivery-acknowledgement delay. Passive stream-accept
polling is fixture scheduling, not an EOS gate. Zero-byte backend data events
carry no protocol bytes and are ignored; their separate FIN remains forwarded.
It is not a cache/cluster implementation or a replacement for native relay tests.

`DiagnosticsTest` exercises real Sink setup failure in an isolated network
with a synthetic authorization value. Captured pipeline/error diagnostics
contain `#MOQX.Secret<REDACTED>` and the connection error, not the synthetic
credential. This covers explicit wrapped authorization, not token-bearing URLs
or arbitrary peer-supplied error text. No real credentials are used by the test.

`test/integration/lite_roundtrip_test.exs` passed both tests against the local
pinned relay and the public relay:

1. Raw plugin Sink → relay → plugin Source: exact final payload and PTS before
   EOS, controlled admission for a pre-registered track, discovery composition.
2. HANG Sink → relay → CatalogSource → explicitly selected Source → legacy
   framing decoder: exact Opus packet and PTS, final packet before EOS, and
   catalog offer withdrawal at track completion.

The second path selects an offer instead of auto-linking every rendition.
Neither test delays EOS or waits for receiver delivery before publishing EOS.
They do not certify absent-track provisioning or every multi-subscriber case.

A later committed-revision rerun passed locally but the public HANG catalog
subscription reset before discovery (code `91141958510842`); raw Lite passed.
The HANG harness had treated publisher-local track readiness as remote broadcast
visibility. It now observes `BroadcastAvailable` before starting CatalogSource,
as the raw test already did. The same-seed rerun and four subsequent public
runs passed both tests. This is an observable discovery barrier, not a sleep,
retry loop, or proof that every distributed-relay failure is resolved.

## Actual reference-player decoding

The runnable harness in `scripts/interop/` publishes encoded H.264 and Opus
through explicit `Hang.Legacy` filters and the plugin Sink. The browser runs
the unmodified reference `moq-watch` component. Observations come from its
video decoder and an analyser connected to its decoded audio graph.

| Receiver path | Decoded video frames | Video timestamp | Audio timestamp | Audio peak |
|---|---:|---:|---:|---:|
| Pinned local relay | 21 | 2000 ms | 2013 ms | 0.0889360 |
| Public cdn.moq.dev relay | 21 | 2000 ms | 1936 ms | 0.0889360 |

Both runs observed multiple increasing timestamps and non-zero audio samples,
with AudioContext `running`. This is decoded-output evidence, not an inference
from handshake, catalog availability, encoded bytes, or AudioContext time.
The harness counts distinct actual `VideoFrame` outputs. It does not use the
reference player's `stats.frameCount` as a decoded count: that counter increments
before `decoder.decode`, despite its API comment describing decoded frames.
Playback permission used an isolated test-browser autoplay flag, not disabled
TLS validation. Incidental HTTP 404s did not prevent decoded playback.

## CMAF H.264/AAC reference-player decoding

The same pinned browser/relay stack also passed with real FFmpeg-generated
fragmented MP4: separate H.264 video and AAC stereo tracks, complete `moof`/`mdat`
pairs, and initialization carried in the HANG catalog. Publication used the
explicit `Hang.CMAF` adapter; no legacy prefix or MP4 retiming was introduced.

| Receiver path | Actual decoded video frames | Video timestamp | Audio timestamp | Audio peak |
|---|---:|---:|---:|---:|
| Pinned local relay | 20 | 1900 ms | 1981.6667 ms | 0.0909207 |
| Public cdn.moq.dev relay | 20 | 1900 ms | 1987.0833 ms | 0.0909332 |

An initial run reported silence because the analyser remained connected to the
player's retired startup audio graph. The harness now follows changes to the
public audio root. The successful reruns measured the active decoded graph.
No plugin or reference decoder changes were needed for this correction.

## Reverse reference publication → plugin receive → independent decode

The pinned Rust `moq import fmp4` published live synthetic H.264/AAC CMAF.
`scripts/interop/receive_cmaf.exs` selected its catalog offers, received media
through plugin CatalogSource/Source and the explicit CMAF FromTrack adapter,
and checked ordered, advancing PTS. FFmpeg independently decoded initialization
plus the unchanged received fragments.

| Path | Chunks per track | Decoded video frames | Decoded audio samples (interleaved) | Signed-16 audio peak |
|---|---:|---:|---:|---:|
| Pinned local relay | 3 | 30 | 288768 | 2902 |
| Public cdn.moq.dev relay | 3 | 30 | 286720 | 3050 |

Local received video PTS advanced from 32021386719 to 34021386719 ns and
audio from 32021333333 to 34026666667 ns. Public video PTS advanced from
29021386719 to 31021386719 ns and audio from 29034666667 to 31040000000 ns.
Both decoder processes exited successfully. This proves reception and actual
decoding of reference-published CMAF, not a real-time decoder inside Source/Sink.
The reverse legacy Opus/H.264 path is not covered by this particular test.

The first local setup used a self-signed CA certificate as the endpoint leaf;
Rust's verifier correctly rejected it. The successful local reference run used
the existing CA-signed server certificate with peer verification, not a TLS bypass.
The reference CLI logged producer-drop warnings at the end of its finite input;
these runs certify media before shutdown, not reference-side graceful EOS.

## Still unverified or incomplete

- Reverse legacy Opus/H.264 receive/decode remains unverified; reverse CMAF is
  verified above.
- Discovery initial snapshot, live add/remove/reappearance, foreign-owner
  cancellation rejection, owner-exit cancellation, and surviving-owner updates
  have public Session/transport regression coverage. Shared-Source failure,
  demand and resubscription now have full hermetic coverage above; native
  multiple-subscriber certification and discontinuities remain incomplete.
- The pinned relay requests TrackInfo before controlled admission. MOQX 0.9.0
  rejects TrackInfo for absent tracks, so on-demand creation solely in response
  to admission is not certified; register the track first.
  The complete upstream metadata/provisioning dependency is tracked in
  [MOQX #47](https://github.com/dmorn/moqx/issues/47).
- Full-suite stress exposed a draft-16 fixture discarding an early datagram
  while waiting for catalog bytes, and a CMAF fixture subscribing after its
  producer had already emitted EOS. The fixtures now retain early datagrams
  and establish subscriber admission before starting the finite CMAF producer.
  Both focused tests passed eleven consecutive runs afterward; the complete
  suite passed 89 tests with seven opt-in integrations excluded (seed 233865).
  The three new hermetic scenarios also passed eleven consecutive runs.
- Empty-group codec-epoch discontinuities are unsupported: Source emits no
  discontinuity event and MOQX 0.9.0 has no empty-group publication operation.
  A timestamped empty-payload MediaEnd frame is a different concept and is
  covered separately. The complete upstream lifecycle/API dependency is
  [MOQX #48](https://github.com/dmorn/moqx/issues/48).
- Draft implementation [PR #12](https://github.com/kim-company/membrane_moqx_plugin/pull/12)
  is open. Its adversarial review found a raw Cloudflare initialization crash
  and a broader transport/profile initialization mismatch. The raw-mode crash
  has a public-pipeline regression and minimal fix; the cross-profile lifecycle
  design remains open in the review discussion. These observations do not close
  #10 or #11 and do not claim universal HANG/codec/browser support.
