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
| Cloudflare draft-16 / `moqt://draft-16.cloudflare.mediaoverquic.com:443` | `:draft_16` | TLS-verified QUIC/ALPN passed; unauthenticated sessions closed during or just after setup | Authenticated publication, media and EOS remain unverified; do not treat connection success as certification |
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

## Cloudflare draft-16: prerequisite and current check result

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

The authenticated data/EOS check is **blocked pending provisioned relay
credentials and validation of the secret-safe native-QUIC configuration**.
No infrastructure was provisioned and no real credentials were used. Supply
secret-file or secret-manager references rather than token values in chat,
shell arguments, environment values, source or tracker comments. Before using
real tokens, verify URI-path redaction in logs, exceptions and assertions with
synthetic secrets. Cloudflare also warns that token-bearing paths may appear
in server access logs.

[MOQX #42](https://github.com/dmorn/moqx/issues/42) owns that complete upstream
certification. The acceptance run must show exact receiver payloads followed
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
