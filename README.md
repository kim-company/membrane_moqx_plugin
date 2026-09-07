# Membrane MOQX Plugin

Membrane Framework Source and Sink elements for receiving and publishing
arbitrary named object tracks through [`moqx`](https://github.com/dmorn/moqx).

The plugin implements MoQ Lite draft-05, standard MOQT draft-16, and
Cloudflare draft-14 over native QUIC. Implemented protocol support does not
imply compatibility with every current public relay. See the dated
[relay compatibility and retirement policy](docs/relay-compatibility.md):
Cloudflare draft-14 has a known immediate-EOS delivery limitation, authenticated
Cloudflare draft-16 verification is pending, and the current public Moqtail
relay requires draft-18, which MOQX 0.8.1 does not implement.

Protocol selection is always explicit. `moqx` owns transport,
draft-specific wire/lifecycle state, typed events, subscriptions,
publications, and object delivery; this project adapts those public values to
Membrane elements and buffers.

## Status

`Membrane.MOQX.Source` and `Membrane.MOQX.Sink` receive and publish the
canonical `Membrane.MOQX.Track` format through the public `moqx` API. Explicit
format adapters convert CMAF and custom Membrane formats at neighboring
pipeline boundaries. `Membrane.MOQX.CatalogSource` adds catalog discovery and
pipeline-controlled dynamic track attachment over one shared MOQX session.

The first version will rely on Membrane's Toilet for pipeline overload handling.
Protocol-neutral demand credits and bounded delivery inside `moqx` are deferred
until real pipeline usage demonstrates that they are needed.

## Track boundary

MOQT transports opaque named objects. Core Sources and Sinks do not assume that
a track contains audio or video. They exchange `Membrane.MOQX.Track` stream
formats and buffers with `Membrane.MOQX.Unit` metadata. A track contains an open
string packaging identifier, optional initialization bytes, generic selection
parameters, and additional JSON-compatible catalog fields.

Format adapters translate that canonical contract without changing payload
bytes. Pipelines that need frame-level H.264 or AAC buffers compose the
appropriate CMAF/MP4 muxer or demuxer outside the core elements.

## Elements

- `Membrane.MOQX.Source` subscribes to exactly one selected track and emits its
  payload bytes unchanged with received MOQ coordinates, status, and inferred
  group boundaries in `Membrane.MOQX.Unit` metadata.
- `Membrane.MOQX.CatalogSource` owns a shared relay session, reports available
  tracks to its parent, and creates one internal `Source` only when the pipeline
  links the corresponding dynamic output pad. A pipeline may also request an
  unadvertised track by supplying its canonical stream format on that pad.
- `Membrane.MOQX.Sink` advertises a namespace and, for catalog-bearing
  protocols, a retained full catalog. Each
  requested input pad accepts one canonical `Membrane.MOQX.Track`. The Sink
  assigns MOQ coordinates and finishes the namespace publication when it
  terminates.
- `Membrane.MOQX.TrackAdapter.ToTrack` and `FromTrack` are explicit filters for
  converting concrete Membrane formats to and from the canonical contract.

Authorization tokens remain explicit caller input and are passed to `moqx` as
redacted `MOQX.Secret` values.

The default catalog track follows the selected protocol: `catalog` for
`:draft_16` and `.catalog` for `:cloudflare_draft_14`. MoQ Lite exact-track
operation publishes no catalog. Set
`catalog_track_name` explicitly to override either convention. Endpoints and
failed negotiation never select a protocol or catalog schema implicitly.

## Subscribing

Use `Membrane.MOQX.Source` when the track address and canonical format are
already known:

```elixir
child(:source, %Membrane.MOQX.Source{
  endpoint: "moqt://draft-14.cloudflare.mediaoverquic.com:443",
  protocol: :cloudflare_draft_14,
  track: %MOQX.TrackRef{
    namespace: ["my-service", "camera-1"],
    track: "events"
  },
  stream_format: %Membrane.MOQX.Track{
    packaging: "application/example",
    initialization: nil
  }
})
```

The same element subscribes through standard draft-16. Supply
`draft16_endpoint` for an explicitly compatible relay; the current public
`relay.moqtail.dev` is not a draft-16 target. MOQX subscription options are
passed through unchanged:

```elixir
child(:source, %Membrane.MOQX.Source{
  endpoint: draft16_endpoint,
  protocol: :draft_16,
  track: %MOQX.TrackRef{
    namespace: ["moqtail", "testsrc"],
    track: "video"
  },
  stream_format: discovered_or_known_track,
  subscription_options: [
    start: :next_group,
    priority: 127,
    group_order: :ascending,
    delivery_timeout: 5_000
  ]
})
```

Use `Membrane.MOQX.CatalogSource` when the pipeline should discover possible
tracks and decide which ones to attach. Catalog entries produce
`{:track_available, %Membrane.MOQX.TrackOffer{}}` parent notifications; they do
not modify the graph automatically. Linking an exact dynamic output pad starts
its one-track subscription.

Catalog membership is discovery, not admission. To request a track before it
is advertised, link a dynamic output pad with `track` and `stream_format`
options. This sends the subscription immediately and allows a remote
pull-based publisher to provision the track on demand.

For draft-16 CMSF catalogs, decoded inline `initData` becomes
`Membrane.MOQX.Track.initialization`; no initialization subscription is
created. Cloudflare catalogs retain the separate `initTrack` subscription
path.

## MoQ Lite draft-05

MoQ Lite is an explicit transport and publication protocol here; it is not a
claim that the payload implements HANG. Exact known-track subscription is the
supported contract. The plugin does not synthesize a CMSF or HANG catalog, and
it does not create a separate initialization track.

A Source receives the immutable track timescale from `TRACK_INFO`. Frame
timestamps are converted to Membrane nanoseconds independently of group and
object identifiers:

```elixir
child(:source, %Membrane.MOQX.Source{
  endpoint: "moql://cdn.moq.dev:443",
  protocol: :moq_lite_05,
  track: %MOQX.TrackRef{namespace: ["my-service", "speech"], track: "opus"},
  stream_format: %Membrane.MOQX.Track{packaging: "opus", initialization: nil}
})
```

For publication, every Lite media pad requires a positive `timescale`.
`publisher_priority`, `publisher_max_latency`, `retention`, and reliable
`:subgroup` delivery are explicit pad policy. Membrane PTS is rounded to the
nearest track tick, with exact halves rounded up. PTS must be non-negative and
non-decreasing; equal PTS values and distinct values that quantize to the same
tick are accepted.

```elixir
child(:producer, producer)
|> via_in(Pad.ref(:input, :opus),
  options: [
    track_name: "opus",
    timescale: 48_000,
    publisher_priority: 127,
    publisher_max_latency: 250,
    retention: :live,
    delivery: :subgroup
  ]
)
|> child(:moqx_sink, %Membrane.MOQX.Sink{
  endpoint: "moql://cdn.moq.dev:443",
  protocol: :moq_lite_05,
  namespace: ["my-service", "speech"],
  inbound_subscriptions: :controlled,
  track_demand_events: true
})
```

In controlled mode, accept the typed request and then add the dynamic input
pad whose `track_name` exactly equals `request.track.track`. The first accepted
subscriber emits active `TrackDemand`; the final departure emits inactive
`TrackDemand`. Reaching zero demand does not remove the pad, so later
subscriptions reuse the same producer.

## Publishing CMAF

The built-in CMAF adapter supports AAC and H.264 tracks. `ToTrack` converts
`Membrane.CMAF.Track` and CMAF buffer metadata into the canonical format before
the Sink. One input buffer becomes one MOQ object without changing its payload.
Absent `metadata.last_chunk?` means the buffer is a complete segment.

```elixir
import Membrane.ChildrenSpec

alias Membrane.MOQX.{Sink, TrackAdapter}
alias Membrane.Pad

require Pad

source
|> child(:cmaf_to_moqx, %TrackAdapter.ToTrack{adapter: TrackAdapter.CMAF})
|> via_out(Pad.ref(:output, :video))
|> via_in(Pad.ref(:input, :video),
  options: [
    track_name: "video.m4s",
    init_track_name: "video.init.mp4",
    retention: :latest
  ]
)
|> child(:moqx_sink, %Sink{
  endpoint: "moqt://draft-14.cloudflare.mediaoverquic.com:443",
  protocol: :cloudflare_draft_14,
  namespace: ["my-service", "camera-1"]
})
```

Compose an encoder and CMAF muxer upstream when starting from raw AAC or H.264.
The Sink does not encode, mux, split, inspect, pace, or loop media samples.

For standard draft-16 publication through a compatible relay, provide CMSF
metadata as top-level selection/catalog fields. Initialization stays inline in
the catalog:

```elixir
stream_format = %Membrane.MOQX.Track{
  packaging: "cmaf",
  initialization: cmaf_initialization,
  selection_params: %{
    "codec" => "avc1.42C01F",
    "width" => 640,
    "height" => 360
  },
  catalog_fields: %{
    "role" => "video",
    "timescale" => 90_000,
    "bitrate" => 800_000
  }
}

source
|> via_in(Pad.ref(:input, :video),
  options: [
    track_name: "video",
    retention: :all,
    delivery: :subgroup
  ]
)
|> child(:moqx_sink, %Membrane.MOQX.Sink{
  endpoint: draft16_endpoint,
  protocol: :draft_16,
  namespace: ["my-service", "camera-1"],
  catalog_refresh_interval: 1_000
})
```

Set a pad's `delivery: :datagram` to publish draft-16 object datagrams.
`:subgroup` is the default. Cloudflare draft-14 remains subgroup-only and
rejects datagram publication through MOQX.

### Custom track adapters

Formats other than `Membrane.CMAF.Track` can implement the public
`Membrane.MOQX.TrackAdapter` behaviour. Directional callbacks convert stream
formats and buffers to or from `Membrane.MOQX.Track` and
`Membrane.MOQX.Unit`. Select the module on an explicit neighboring filter:

```elixir
child(:custom_to_moqx, %TrackAdapter.ToTrack{adapter: MyApp.MOQXTrackAdapter})
|> via_out(Pad.ref(:output, :custom))
|> via_in(Pad.ref(:input, :custom), options: [track_name: "custom.media"])
```

Adapters translate media descriptions and unit metadata only. Core Sources and
Sinks remain the owners of subscriptions, publications, MOQ coordinates,
catalog state, and lifecycle. Adapter selection is explicit in the children
spec; there is no global registry or Application-environment lookup.

The canonical format is not limited to media. A subtitle or application track
can be supplied directly:

```elixir
%Membrane.MOQX.Track{
  packaging: "webvtt",
  initialization: nil,
  selection_params: %{"mimeType" => "text/vtt", "lang" => "it"},
  catalog_fields: %{"label" => "Italian subtitles"}
}
```

Each corresponding buffer carries `%Membrane.MOQX.Unit{group_end?: boolean}`
under `buffer.metadata.moqx`. The Sink assigns fresh MOQ coordinates; received
coordinates on Source buffers remain available for observation and adapters.

## Pull-based publishing

Inbound subscriptions are accepted automatically by default. Set
`inbound_subscriptions: :controlled` when the pipeline must authorize requests
or provision tracks only after they are requested:

```elixir
child(:moqx_sink, %Membrane.MOQX.Sink{
  endpoint: endpoint,
  protocol: :cloudflare_draft_14,
  namespace: ["my-service", "channel"],
  inbound_subscriptions: :controlled,
  subscription_decision_timeout: 5_000,
  max_pending_subscriptions: 128
})
```

The Sink notifies its parent with the unchanged, typed MOQX request:

```elixir
{:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request}
```

The parent decides through a child notification:

```elixir
Membrane.Pipeline.notify_child(pipeline, :moqx_sink, {:accept_subscription, request})

Membrane.Pipeline.notify_child(
  pipeline,
  :moqx_sink,
  {:reject_subscription, request,
   %MOQX.SubscriptionRejection{code: :unauthorized, reason: "not allowed"}}
)
```

Acceptance and track readiness are independent. If the requested track is not
registered yet, approval remains pending. The parent can create a producer and
link one dynamic Sink pad whose `track_name` matches `request.track.track`.
Once its canonical stream format arrives, the Sink registers the track and
accepts every approved request waiting for it. Multiple subscribers therefore
share one logical pad and producer.

For draft-16, the Sink waits for namespace readiness and each track's
`PUBLISH_OK` readiness before publishing catalogs, initialization, or media.
Publisher track readiness is not counted as a remote subscriber and does not
emit `TrackDemand`. Actual accepted subscribers alone drive join/leave counts
and zero/nonzero demand transitions.

Pending unsubscribe, decision timeout, publication finish, and publication
cancellation produce:

```elixir
{:subscription_cancelled, request, reason}
```

Invalid, stale, or failed parent decisions produce:

```elixir
{:subscription_decision_failed, request, reason}
```

Accepted subscription lifecycle notifications include an opaque identity and
the current count for that track:

```elixir
{:subscriber_joined, track_name, identity, subscriber_count}
{:subscriber_left, track_name, identity, subscriber_count}
```

The identity remains the original request handle in controlled mode. The Sink
retains MOQX's separate opaque published-subscription handle internally, so the
parent can finish exactly one accepted subscriber without withdrawing the
track or namespace:

```elixir
Membrane.Pipeline.notify_child(
  pipeline,
  :moqx_sink,
  {:finish_subscription, identity,
   [status: :subscription_ended, reason: "operator removed subscriber"]}
)
```

The resulting MOQX terminal event produces the ordinary
`{:subscriber_left, ...}` notification and updates aggregate demand. Ending or
removing a pad finishes every active subscription for that track with
`:track_ended`; subscriptions on other tracks remain active. A remote
unsubscribe racing that cleanup is treated as the same terminal lifecycle.

In controlled mode the catalog and known initialization tracks are accepted
automatically by default. Set `infrastructure_subscriptions: :controlled` to
route those requests through the parent too.

Set `track_demand_events: true` to emit
`Membrane.MOQX.Event.TrackDemand` upstream on a media pad when its subscriber
count crosses `0 -> 1` or `1 -> 0`. This event is an optional production
optimization signal. Authorization and dynamic graph changes remain parent
notification responsibilities, and the Sink never removes a pad merely
because its count reaches zero.

## Installation

Until the package is published to Hex, depend on the Git repository:

```elixir
def deps do
  [
    {:membrane_moqx_plugin,
     github: "kim-company/membrane_moqx_plugin",
     branch: "main"}
  ]
end
```

## Development

```bash
mix deps.get
mix test
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
```

Hermetic element tests should use `MOQX.Testing.Transport`. Live Cloudflare and
Moqtail checks remain explicitly selected integration tests.

### MoQ Lite native-QUIC validation

`test/integration/moq_lite_05_test.exs` exercises two subscribers, first and
final explicit departures, later resubscription, an abrupt final connection
close, bounded leave notification, timestamps, and retained topology through
the public Membrane and MOQX APIs.

The local gate uses the Curley relay pinned by `moqx` v0.8.0 certification to
commit `fd477082c43c3c0738fb62d077d85ea078f10045`. Start that release's
`curley-moq-lite-05-relay` service on UDP 4463, then run:

```bash
MOQX_LITE_CA_FILE=/path/to/moqx/.tmp/integration-certs/ca.pem \
  mix test --include integration test/integration/moq_lite_05_test.exs
```

The public gate is opt-in and uses a unique anonymous broadcast path:

```bash
MOQX_LITE_ENDPOINT=moql://cdn.moq.dev:443/anon \
MOQX_LITE_CA_FILE=/etc/ssl/cert.pem \
  mix test --include integration test/integration/moq_lite_05_test.exs
```

### Live Cloudflare validation

These tests select `:cloudflare_draft_14` explicitly. Changing the endpoint
does not change their protocol. The Source test currently fails against the
public draft-14 relay when EOS follows the final buffer immediately; see
[the evidence and successor-verification gate](docs/relay-compatibility.md).
Do not insert a delay or wait for receipt before EOS to declare this gate green.

Live relay tests are excluded from normal test runs. The Source test exercises
an unadvertised subscription, controlled Sink provisioning, payload delivery,
and EOS through Cloudflare:

```bash
mix test --only integration test/integration/cloudflare_source_test.exs
```

The Sink test uses a real fragmented H.264 MP4 fixture:

```bash
MOQX_CMAF_FIXTURE=/tmp/input-fragmented.mp4 \
  mix test --only integration test/integration/cloudflare_sink_test.exs
```

Both tests default to Cloudflare's public draft-14 relay. Override
`MOQX_ENDPOINT` only for another compatible draft-14 relay.
`MOQX_AUTHORIZATION_FILE` loads a token into `MOQX.Secret` for that existing
draft-14 authorization path. It is not a verified Cloudflare draft-16 session
authentication recipe. Cloudflare documents draft-16 tokens in the connection
URL path; authenticated verification and secret-safe configuration are tracked
in [MOQX #42](https://github.com/dmorn/moqx/issues/42).

### Live Moqtail draft-16 validation

The historical default `relay.moqtail.dev` now rejects draft-16. Set
`MOQX_DRAFT16_ENDPOINT` to a caller-started, pinned compatible relay for each
command below. Record the relay commit/image digest and TLS configuration.
Changing only ALPN is not a protocol upgrade.

The Source check requires that relay to already host a `moqtail/testsrc`
namespace with a CMSF catalog and a live H.264 track. It preserves inline CMAF
initialization, dynamically attaches the offered track, and receives media:

```bash
MOQX_DRAFT16_ENDPOINT=moqt://your-pinned-draft16-relay:443 \
  mix test --include integration \
  test/integration/moqtail_draft_16_test.exs:16
```

The publication check uses synthetic WebVTT data and exercises a controlled,
namespace-routed subscription through the Sink and MOQX. It needs neither a
CMAF fixture nor a pre-existing media publisher. It does not prove H.264
playback or immediate final-object/EOS delivery:

```bash
MOQX_DRAFT16_ENDPOINT=moqt://your-pinned-draft16-relay:443 \
  mix test --include integration \
  test/integration/moqtail_draft_16_test.exs:70
```

For an operator-run player smoke, use a player and relay pinned to mutually
compatible draft-16 versions. The current public `player.moqtail.dev` /
`relay.moqtail.dev` workflow is not certified with MOQX 0.8.1; successor work
is tracked in [MOQX #43](https://github.com/dmorn/moqx/issues/43).

1. Use a paced producer of fresh, monotonically timestamped CMAF fragments;
   do not loop a static MP4 timeline through the Sink.
2. Start the draft-16 Sink example and note its namespace.
3. Open the matching player, select the same draft-16 relay endpoint, and
   enter that namespace.
4. Record catalog discovery, `Playing`, the decoded resolution, and advancing
   playback.
5. Terminate the Membrane pipeline and confirm the publication is withdrawn.

Catalog refresh is enabled for draft-16 by default so a player attaching late
can discover the current retained catalog. The refresh timer is cancelled on
normal termination, publication failure/cancellation, protocol failure, and
connection close. Set `catalog_refresh_interval: nil` to disable it.
