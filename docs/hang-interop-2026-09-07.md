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

`test/integration/lite_multi_source_test.exs` additionally passed on both the
pinned local relay (seed 58877, 0.3 seconds) and public relay (seed 63983,
1.1 seconds). Two real plugin Sources share one Session and both receive exact
media/PTS; killing the first Source leaves the second receiving. Killing the
last Source returns upstream demand to zero. A third Source subscribes on the
same Session with an explicit group-start filter and receives new media before
immediate EOS, with exact group/object coordinates.

The reference relay combines downstream subscriptions into one upstream
subscription. Sink subscriber counts therefore describe its MOQX connection,
not downstream viewer counts. These native tests observe broadcast discovery
before subscribing and add no EOS delay or receiver-delivery acknowledgement.

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

## Catalog failure/recovery addendum — 2026-09-08

Public-pipeline regressions using the in-memory Lite transport now verify:

- A rejected HANG catalog subscription reports the typed MOQX subscription
  error, terminates the catalog child, and leaves its crash-group-isolated
  parent pipeline alive.
- Connection loss reports its close reason and terminates the catalog child.
  The same parent pipeline can then create a fresh catalog child against a new
  endpoint. This is parent-managed replacement, not transparent reconnection.
- Malformed JSON reports a catalog error without dropping valid offers. A
  subsequent identical valid snapshot introduces no duplicate offer events,
  and a later empty snapshot removes the original offer.

The catalog suite passed three consecutive runs (13 tests each). The full
suite passed 93 tests with eight opt-in integrations excluded (seed 107537),
and strict Credo found no issues. These regressions required fixture capabilities
for subscription rejection and connection close, but no production behavior fix.
They do not certify every linked-media failure or same-endpoint recovery path.

## Reverse legacy addendum — 2026-09-08

The same pinned moq-dev/moq reference commit's browser publisher consumed a
synthetic H.264/AAC MP4 file and re-encoded H.264/Opus into HANG legacy. No camera
or microphone was used. Chrome was **152.0.7977.76** for these new runs; this
does not change the browser version recorded for the earlier forward tests.
CatalogSource selected the tracks, Source received them, and the explicit
Legacy adapter produced codec packets. Independent WebCodecs decoders then
decoded those captured public pipeline outputs.

| Path | Decoded video frames | Decoded audio frames per channel | Float audio peak | Reordered audio packets |
|---|---:|---:|---:|---:|
| Pinned local relay | 25 | 244800 | 0.0949336 | 0 |
| Public cdn.moq.dev relay | 25 | 228480 | 0.1020594 | 3 |

Both runs required an initial video keyframe and subsequent delta frames.
Local decoded video timestamps advanced from 2569443 to 4169218 microseconds,
audio from 1485564 to 6565564. Public video advanced from 2570586 to 4170406,
audio from 1554974 to 6294974. These are independently collected track windows,
not a claim of A/V synchronization.

An initial harness run used a nonexistent reference signal; it was corrected
to the public `out.catalog`. Another setup mistakenly requested keyframes every
millisecond; final runs use a one-second interval and explicitly require delta
frames. The first public attempt incorrectly asserted globally ordered arrival
PTS. Coordinate diagnostics showed independent Opus groups arriving out of order
(for example group 56 before 55), consistent with the Source contract. The final
capture preserves arrival order, checks contiguous objects and ordered PTS
within each group, and the **offline decoder** orders groups/objects, reporting
changed packet positions. The plugin itself does not reorder or pace media.
The pinned reference watch decoder likewise uses a container reorder budget.

The durable harness is `receive_legacy.exs` plus `browser/publish.html`,
`browser/publish.ts` and `browser/check-reverse.mjs`, with commands in the interop
README. TLS peer verification remained enabled locally and publicly. The browser
closes after the finite capture/decode check; this is not graceful publisher EOS,
bounded-latency playback or loss-recovery certification. No production behavior
change was needed. The full suite passed 93 tests with eight opt-in integrations
excluded (seed 98383); strict Credo passed.

## Catalog deselection addendum — 2026-09-08

Catalog pad-removal regression (2026-09-08): a full HANG pipeline uncovered an
invalid `remove_child` Bin action when deselecting media. The fix uses Membrane's
supported `remove_children` action. The public-pipeline test now receives media,
removes the linked adapter/sink, observes publisher media demand return to zero,
reselects the same catalog pad and receives new media. A subsequent catalog
withdrawal still reaches the parent after both media selections are removed,
proving catalog subscription independence. The focused regression passed six
consecutive runs. This is explicit deselection/reselection, not automatic
recovery from a crashing linked Source.
The full suite passed 94 tests with eight opt-in integrations excluded (seed
551623), and strict Credo passed.

## Still unverified or incomplete

Approved initialization implementation (2026-09-08): initialization now follows
the selected profile, including Moqtail inline metadata over Cloudflare and
Cloudflare separate initialization over draft-16/Lite. Public regressions exposed
the previous nil-track crash, missing Lite timestamps, and draft-16 publication
before initialization readiness. The implementation uses valid Lite timing/FIN
and event-driven readiness of both initialization/media tracks. Draft-16 initial
and changed-generation capture passes, and all 35 Sink tests passed (seed 326769).
Inline/separate Lite catalog update tests pass in focused runs, but full/stress
runs expose a distinct MOQX 0.9.0 defect: unsubscribe after an initialization
object followed by peer group FIN raises `unknown_group_subscription`. A minimal
public MOQX reproduction without CatalogSource/Sink confirms this. Upstream group
lifecycle work owns the fix; release certification remains blocked until a
released MOQX fix is consumed. No dependency patch or delay workaround is used.

Approved track-isolation implementation (2026-09-08): selected Sources now have
individual temporary crash groups. A public HANG lifecycle regression verifies
rejected selection reporting, surviving sibling media, parent-driven same-pad
replacement, cancellation/zero demand, and continuing catalog updates. It passed
eleven consecutive runs; catalog/lifecycle suites passed 18 tests (seed 571351).
This supersedes the Bin-wide selected-Source failure limitation recorded below.
Membrane removes the failed output pad; parents handle child-pad removal and
isolate the affected downstream branch. There is no automatic retry, and genuine
catalog/session failure still terminates the Bin.

Discovery/catalog composition addendum (2026-09-08): the native HANG roundtrip
now verifies that media EOS removes its catalog offer while a fresh discovery
snapshot still sees the broadcast. Only closing the publisher produces
`BroadcastWithdrawn` on both the original and late discovery handles; explicit
cancellation then yields scoped `DiscoveryDone` events. The extended test passed
on the pinned local relay (seed 89135, 5.3 seconds) and public anonymous relay
(seed 118207, 6.1 seconds), with TLS verification and no EOS grace sleeps.
Strict Credo and formatting/diff checks passed. No production change was needed.

Selected-media rejection coverage (2026-09-08): selecting a caller-described
track through CatalogSource and receiving a real Lite subscription RESET
forwards `{:track_source, ref, {:subscription_failed, ref, protocol_error}}`
before the CatalogSource terminates. The current Bin does not isolate child
Sources. An initial test isolated only the catalog: its static-pad consumer
then crashed on unlink during setup. Isolating both in parent-managed crash
groups preserved the outer pipeline; the regression passed four consecutive
runs. This documents existing failure propagation, not per-track recovery or
survival of sibling selections. Changing that fault boundary requires a
maintainer design decision.
Full-suite verification passed 95 tests with eight opt-in integrations excluded
(seed 746932), and strict Credo passed.

- Reverse legacy Opus/H.264 and reverse CMAF receive/decode are verified above;
  real-time playback ordering, synchronization and loss recovery are not
  provided by the core Source or framing adapters.
- Discovery initial snapshot, live add/remove/reappearance, foreign-owner
  cancellation rejection, owner-exit cancellation, and surviving-owner updates
  have public Session/transport regression coverage. Shared-Source failure,
  demand and resubscription have hermetic and native local/public coverage
  above. Absent-track controlled admission and discontinuities remain blocked
  as described below; this does not certify every catalog failure/reuse path.
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

Upstream candidate validation addendum (2026-09-08):

- [MOQX PR #49](https://github.com/dmorn/moqx/pull/49) at `f00f996`
  supplies empty groups and canceled-subscription group-tail isolation. An
  isolated plugin checkout using that source candidate passed 99 tests with
  eight integrations excluded (seed 317462); five independent full-suite
  repeats also passed. An earlier intermittent failure remains unattributed;
  passing repeats do not establish its cause or resolution.
- [MOQX PR #50](https://github.com/dmorn/moqx/pull/50) at `99f3fca`
  supplies bounded absent-track metadata provisioning. Combining the two
  candidates required no textual conflict resolution. The combined upstream
  suite passed 303 tests with 41 integrations excluded (seed 90217) on repeat.
  Its first run failed the existing draft-16 catalog test with
  `unknown_track_alias`; that test passed in isolation. This is not a claim of
  repeat-stable combined validation.
- These checks use isolated source candidates, not a published dependency.
  This plugin still consumes MOQX 0.9.0. Neither upstream PR nor these test
  results deliver the plugin's missing metadata-demand or discontinuity API;
  released-dependency integration, regression coverage and module documentation
  remain necessary before closing #10 or #11.
- Upstream PR #49's documentation follow-up `b8629db` records actual pinned
  reference-player Opus decoding: 150 `AudioData` outputs across 5s, 10s and 0s
  epochs, resets after outputs 50 and 100, and non-silent PCM. Local and public
  runs passed, but an earlier public run produced an extra reset and a decoder
  `DataError`; its cause remains unknown. This is direct MOQX candidate proof,
  not a plugin Source/Sink discontinuity roundtrip or stable public certification.
- PR #50 review confirmed that a failed TrackInfo send can return a registration
  error after committing the track, without returning its handle; retry then
  reports `track_already_registered`. The public failure-injection regression
  and [review finding](https://github.com/dmorn/moqx/pull/50#discussion_r3956433236)
  make this a remaining API-contract release gate, not merely a missing log.

Approved upstream failure-path fixes (2026-09-08): PR #50 `0899ca9` now returns
the locally registered track handle independently of metadata reply success.
Each request reports its actual send-admission result; failed cleanup attempts
do not suppress that outcome. PR #49 `8293d92` also isolates late TRACK_INFO and
SUBSCRIBE responses after rejection or local cancellation, preserving an active
catalog sibling. Both fixes were independently reviewed with no new actionable
findings; the registration-handle review finding was verified and resolved.

A fresh isolated merge of these exact heads passed 315 upstream tests with 41
integration exclusions (seed 331579), followed by four native local-relay tests
for metadata provisioning/rejection/timeout and empty groups (seed 409526,
verified TLS). The plugin against that combined source candidate passed all
101 tests with eight integration exclusions (seed 331579). These observations
supersede the unresolved registration defect above, but do not explain every
earlier intermittent failure or certify public-browser reset stability. Neither
upstream PR is merged/released by this work; the real plugin dependency remains
MOQX 0.9.0 and the remaining plugin feature integrations are still outstanding.

## Released dependency and plugin integration (2026-09-08)

This addendum supersedes the dependency and missing-feature gates in the
historical checkpoints above, without retroactively changing their results.
MOQX PRs #49/#50 are merged, followed by release PR #51. Published
[MOQX 0.10.0](https://hex.pm/packages/moqx/0.10.0) is tagged at
`36859732f8faaed520352c6c34da7834e3c885f4`; its release workflow succeeded.
The plugin now resolves the official Hex archive, not a path override or
candidate source checkout.

- Sink exposes bounded opt-in missing-track metadata provisioning, scoped
  completion/rejection notifications and separate subscription admission.
  Public-pipeline tests cover accept, reject, timeout and stale decisions.
- Empty groups are preserved as `Event.EmptyGroup` through Source and HANG
  framing, distinct from zero-byte objects, MediaEnd and EOS. Sink publishes
  them in input order, including its initialization FIFO. Native HANG selection
  verifies a backwards timestamp epoch and a final empty group before EOS.
- HANG catalogs support explicit plain or DEFLATE encoding with matching
  catalog names. Live and late readers, concurrent consumers, recognized
  configuration replacement, unknown codecs and removal have regression tests.
- Source/Session expose owner-scoped runtime subscription updates; CatalogSource
  routes selected-pad commands. Local command admission and asynchronous peer
  acknowledgement/rejection are separate contracts. These have hermetic public
  wire/pipeline coverage, not a claim of native relay update certification.
- The released-dependency hermetic suite passed 119 tests with 12 integrations
  excluded (seed 898561); strict Credo was clean. Native epoch/raw/shared-Source
  suites passed four tests locally (770054) and publicly (296210). Native
  metadata provisioning passed three tests locally (73842) and publicly (428015).
- The pinned reference player decoded plugin-published Opus through three real
  epochs (5s, 10s, 0s): 150 non-silent `AudioData` outputs, resets exactly after
  outputs 50 and 100, three configurations and zero browser/decoder errors.
  One local run and three consecutive public cdn.moq.dev runs passed. Unlike
  the earlier direct-MOQX candidate experiment, the publisher is the real
  `TestControlledSource -> Hang.Legacy -> Sink` pipeline. Receiver ordering and
  decoder reset logic remain the reference player's responsibility.
- An initial local harness run decoded its first epoch but failed on HTTP 404
  console errors. Giving the new page a data favicon removed the fixture error;
  the failed trace was retained. These passing runs do not establish a cause
  for the earlier candidate-only public decoder failure.

Artifacts for this checkpoint were retained under the task-local
`/private/tmp/plugin-0.10-proof.LID7BS`: `local-epochs.json` (failed fixture),
`local-epochs-trace.json`, and `public-epochs.json`, `public-epochs-2.json`,
`public-epochs-3.json` (passing). Reproduction scripts are committed; temporary
artifacts are not a portable evidence archive. The public relay's deployed
revision remains unknown. Optional Lite operations/bindings, LOC, untested
codecs, real-time A/V synchronization and universal player compatibility remain
outside the supported matrix in `Membrane.MOQX`.
