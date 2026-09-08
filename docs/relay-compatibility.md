# Relay compatibility and protocol retirement

Evidence date: **2026-09-07**. Consumer baseline: plugin main
`9cf0403190f4c4e9de24e8dda4bdad1ae86bf9f8`, Hex MOQX **0.8.1**.
Public services can change independently of a package release. These results
are a dated observation, not a continuing availability guarantee.

## Implemented support versus verified interoperability

| Protocol / relay | Implementation | Observed result | Guidance |
| --- | --- | --- | --- |
| MoQ Lite 05 / `moql://cdn.moq.dev:443/anon` | `:moq_lite_05` | Public native-QUIC smoke passed, including demand changes, resubscription, timestamps and abrupt final departure | Exact-track operation; no synthesized catalog or claim of HANG media compatibility |
| Cloudflare draft-14 / `moqt://draft-14.cloudflare.mediaoverquic.com:443` | `:cloudflare_draft_14` | Media can flow, but immediate final-buffer/EOS failed | Retained with a known completion limitation; no new draft-14-specific workaround planned absent a concrete consumer requirement |
| Cloudflare draft-16 / `moqt://draft-16.cloudflare.mediaoverquic.com:443` | `:draft_16` | Authenticated controlled Sink-to-Source publication works, but immediate EOS lost the payload in 3 of 5 runs | Not completion-certified; upgrading from draft-14 does not by itself solve the observed failure |
| Current public Moqtail / `relay.moqtail.dev:443` | No draft-18 implementation in MOQX 0.8.1 | `moqt-16` rejected; TLS-verified `moqt-18` QUIC connection succeeded | Use a pinned draft-16 relay for existing workflows; public interoperability needs draft-18 implementation and delivery proof |
| Cloudflare draft-18 interop offering | No draft-18 implementation in MOQX 0.8.1 | Provider-documented test offering; not exercised by this plugin | Not a globally deployed or plugin-certified target |

Cloudflare [documents draft-14 and draft-16 service and authenticated draft-16
connection paths](https://developers.cloudflare.com/moq/). Its draft-18 offering
is described as interoperability testing ahead of global deployment.
Moqtail's [current protocol description](https://github.com/moqtail/moqtail/blob/main/README.md)
and the [interop registry](https://github.com/englishm/moq-interop-runner)
are discovery sources; record a version/date when using them as test targets.

The baseline hermetic suite passed **64 tests, with 5 integration tests
excluded**. That does not certify any public service or browser player.
Likewise, QUIC handshake, MOQT setup, namespace/track readiness, receiver media
delivery, completion, and advancing player playback are separate proof levels.

### Lite coverage boundary

The passing public Lite smoke uses a plugin Sink and direct MOQX subscribers;
it does not exercise a plugin Source on the receiving side. Source timestamp
and subgroup handling have hermetic coverage. A complete plugin Sink-to-relay-
to-Source lifecycle regression and full Lite capability audit remain missing.
These are tracked in [plugin #10](https://github.com/kim-company/membrane_moqx_plugin/issues/10).

`CatalogSource` currently interprets the draft-14/16 CMSF profiles and rejects
Lite. This is an implementation boundary, not a prohibition on catalogs in
Lite: [HANG](https://doc.moq.dev/draft/moq-hang) defines application-level
`catalog.json` media discovery. Neither interpreting that catalog nor HANG
media/player interoperability is implemented. The module documentation is the
source of truth for these API boundaries; this document records dated proof.

## Cloudflare draft-14: immediate completion can lose the final object

Reproduction on the baseline:

```bash
mise exec -- mix test --include integration \
  test/integration/cloudflare_source_test.exs
```

The test provisions an absent track through a controlled Sink subscription,
sends one buffer, and immediately sends EOS. Readiness/subscription succeed,
but the subscriber can receive completion with `expected_streams: 0` and
`processed_streams: 0` without the expected payload.

Diagnostic comparisons, subsequently reverted:

- Waiting for the subscriber to receive the buffer before sending EOS passed.
  This isolates the terminal ordering problem; it is **not** a valid workaround
  or replacement for the immediate-EOS acceptance test.
- Replacing track withdrawal with the previous finish-subscription operation
  also failed, with a late object producing `unknown_track_alias`. Reverting
  withdrawal therefore does not establish a fix.

MOQX 0.8.1 counts published subgroup streams and includes the count in
`PUBLISH_DONE`. Cloudflare's draft-14 branch at
`c8e176bcf4aec6eb1a503b11783238968f431db6`
[removes the subscription without draining the incoming stream count](https://github.com/cloudflare/moq-rs/blob/c8e176bcf4aec6eb1a503b11783238968f431db6/moq-transport/src/session/subscriber.rs#L271-L277)
and [emits a hardcoded zero count downstream](https://github.com/cloudflare/moq-rs/blob/c8e176bcf4aec6eb1a503b11783238968f431db6/moq-transport/src/session/subscribed.rs#L351-L368).
This is consistent with the live failure, but the deployed relay's exact
revision was not verified. Do not present that source correlation as deployed
binary proof or assume a newer relay is fixed.

Protocol ordering, credential handling and any client mitigation belong in
MOQX; element lifecycle and buffer adaptation belong in this plugin. Do not
hide a relay/protocol completion defect with sleeps in Membrane callbacks.

## Cloudflare draft-16: authentication works, immediate EOS is intermittent

The credential-free transport probe was:

```bash
mise exec -- mix run -e '
case MOQX.Transport.Quicer.connect(
       "draft-16.cloudflare.mediaoverquic.com", 443,
       [alpn: ["moqt-16"], verify: :verify_peer,
        cacertfile: "/etc/ssl/cert.pem"], 5000) do
  {:ok, conn} ->
    IO.puts("TLS_verified_moqt16_QUIC_connected")
    :quicer.close_connection(conn)
  result -> IO.inspect(result)
end'
```

It passed. This probe does **not** perform MOQT setup or prove data delivery.
The CA path is the macOS system bundle used for this run; choose the applicable
trusted bundle on another host, never disable certificate verification.

Three repeated public `MOQX.connect/2` calls without a path closed during
setup with `{:connection_closed_during_setup, %{error_code: 3, initiator: :peer}}`.
An explicit `/` path could briefly return a client, followed by
`{:error, {:connection_closed, :noproc}}` on `MOQX.publish/2`. A deliberately
non-secret invalid path also closed during setup. This is not reliable session
readiness and the error code alone does not identify the cause.

Cloudflare documents scoped publisher/subscriber tokens in the connection URL
path. MOQX's draft-16 codec takes its native-QUIC `CLIENT_SETUP` path from the
endpoint URI. The existing draft-14 `MOQX_AUTHORIZATION_FILE` test helper must
not be advertised as a verified substitute for this session-path contract.

An operator subsequently supplied a publish/subscribe credential. Using it in
the connection path established native-QUIC setup and `PublicationReady`.
Both Membrane clients used that scoped credential in an isolated diagnostic
process. No new relay infrastructure was provisioned.

The check adapted the existing controlled Source/Sink integration workflow to
explicit `:draft_16`, with certificate verification and unique namespaces:

1. Publish the namespace, subscribe to an absent track, accept the typed request,
   and attach a controlled producer to the Sink.
2. Observe namespace readiness, track readiness, subscription readiness and a
   real subscriber join.
3. Submit one literal payload buffer and EOS back-to-back, without waiting for
   receiver delivery. Observe the Source's output in message order; EOS without
   the exact payload is a failure even when completion counters agree.

Across **five authenticated immediate-EOS runs**, two delivered the exact final
payload before EOS and three emitted EOS without it. Failed runs reported
expected/processed stream counts of **1/1, 0/0, 0/0**; successful runs reported
**1/1 and 2/2**. These are a small diagnostic sample, not a failure-rate estimate.
Four comparison runs that waited for payload receipt before submitting EOS all
passed. That comparison demonstrates ordinary delivery and narrows the failure
to termination ordering; it does **not** satisfy the immediate-EOS gate.

This establishes a draft-16 failure in the authenticated end-to-end workflow,
not a conclusively localized MOQX-versus-relay defect. Cloudflare main at
`ab1cfffaf988d11c624d1b73b7e6c72e004aed04`
[also removes subscriptions on incoming completion without draining the count](https://github.com/cloudflare/moq-rs/blob/ab1cfffaf988d11c624d1b73b7e6c72e004aed04/moq-transport/src/session/subscriber.rs#L909-L935).
The deployed binary is unverified; pinned-relay/wire evidence is needed to
attribute and repair the failure. No production code or acceptance test was
changed, and no sleep-based workaround was added.

Subsequent isolated transport tracing reproduced missing payload before the
subscriber's protocol decoder: in one failure the publisher submitted the
payload stream and an empty terminal stream, but the subscriber saw only the
terminal stream and completion. A direct MOQX-only check without Membrane or
the empty terminal object also failed (four runs: two delivered, one completed
without payload, one timed out). The plugin is therefore not necessary to
reproduce this failure; removing its terminal object is not a demonstrated fix.
These temporary diagnostic harnesses are not committed regression coverage.
The deployed relay revision remains unknown. Draft-16 completion can precede
data, and its [completion rules](https://www.ietf.org/archive/id/draft-ietf-moq-transport-16.html#section-9.15)
allow early state disposal at the cost of late objects, so this evidence is a
delivery limitation, not by itself proof of a protocol violation.

Credential-safe execution used non-echoing stdin, in-memory URL construction,
disabled logging in the isolated diagnostic VM, caught failures and fixed
allowlisted output. Synthetic invalid-credential runs checked those failure
diagnostics first. The token was not saved to a file, supplied as a process
argument/environment value, or included in published evidence. This is **not**
a general redaction certification for MOQX, Membrane or application logging.
The one-off harness is not a committed automated regression; a reusable safe
integration workflow remains part of the upstream certification.

For future runs, prefer secret-file or secret-manager references over token
values in chat. Never put token-bearing URLs in shell arguments, environment
values, source, ordinary diagnostics or tracker comments. Validate redaction
with synthetic secrets before enabling normal logs; Cloudflare also warns that
token-bearing paths may appear in server access logs.

[MOQX #42](https://github.com/dmorn/moqx/issues/42) owns that complete upstream
certification; missing credentials are no longer the blocker for this check.
The acceptance run must show exact receiver payloads followed
by EOS when the final buffer and finish are submitted immediately, with no
receipt acknowledgement or arbitrary sleep before finish. It must also cover
withdrawal, departure and reuse; retain the endpoint/version/date and sanitized
receiver evidence. An authenticated handshake alone is insufficient.

## Moqtail: pin the old target or implement the new protocol

Current public Moqtail rejects draft-16 ALPN with `alpn_neg_failure` and accepts
draft-18 at the QUIC handshake level. MOQX 0.8.1 cannot communicate in draft-18.
Changing only ALPN or retrying another protocol implicitly is not supported.

For existing draft-16 tests, explicitly set `MOQX_DRAFT16_ENDPOINT` to a
compatible pinned relay. The historical test default is no longer a working
public target. The catalog test additionally needs an active `moqtail/testsrc`
CMSF/H.264 publisher; the controlled-publication test uses synthetic WebVTT,
not a CMAF fixture. See the [README commands](../README.md#live-moqtail-draft-16-validation).
A matching pinned player is needed for draft-16 playback evidence; a passing
object test alone does not prove decoded/advancing H.264 playback.

[MOQX #43](https://github.com/dmorn/moqx/issues/43) owns the complete draft-18
publisher/subscriber implementation and relay interoperability. Plugin
[issue #4](https://github.com/kim-company/membrane_moqx_plugin/issues/4) remains
the downstream integration/live-proof gate. Historical successful runs remain
historical evidence; do not relabel them as current public compatibility.

## Retirement policy

No protocol is removed or newly declared deprecated by this documentation.
Draft-14 remains selectable with the limitation above; draft-16 remains a
useful target for compatible deployments. MoQ Lite is a separate protocol
family: its draft-05 number cannot be compared with MOQT draft-14/16/18 as an
age or retirement rule.

Before deprecating or deleting a protocol implementation:

1. Identify actual consumers, deployment history and required operations.
   Record whether a production compatibility obligation exists; do not invent
   one, or assume no consumers merely because a public endpoint moved.
2. Certify the replacement using a pinned real-QUIC relay and the intended
   public deployment. Cover authentication, payload/catalog delivery, immediate
   final-object/EOS, cancellation, withdrawal, final departure and reuse.
   Include advancing player proof when the consumer requires playback.
3. Publish migration instructions identifying package versions, explicit
   protocol/endpoint, authentication, catalog/initialization differences,
   capabilities and known gaps. Keep secrets out of examples and evidence.
4. Obtain explicit maintainer approval of the retirement scope and release
   plan: announcement/deprecation, removal version, affected public options,
   and remaining coverage. A clean cut can be approved when appropriate; no
   automatic compatibility facade or fixed grace period is imposed.
5. Land removal separately, with regression coverage for retained protocols,
   updated examples/release notes and clear failure for removed selections.
   Do not remove tests merely to make an unverified replacement appear green.

Existing [#1](https://github.com/kim-company/membrane_moqx_plugin/issues/1),
[#2](https://github.com/kim-company/membrane_moqx_plugin/issues/2) and
[#4](https://github.com/kim-company/membrane_moqx_plugin/issues/4) retain their
unfulfilled implementation/live-proof gates. If successor validation replaces
a historical draft-14 requirement, revise that acceptance scope explicitly;
do not mark the old test as passed or silently close the issue.
