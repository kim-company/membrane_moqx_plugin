# Membrane MOQX Plugin

Membrane Framework Source and Sink elements for receiving and publishing
arbitrary named object tracks through [`moqx`](https://github.com/dmorn/moqx).

The initial integration targets Cloudflare's deployed MOQT draft-14 relays over
native QUIC. `moqx` owns the transport and protocol lifecycle; this project
adapts its typed subscriptions, publications, objects, and terminal events to
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
- `Membrane.MOQX.Sink` advertises a namespace and a retained full catalog. Each
  requested input pad accepts one canonical `Membrane.MOQX.Track`. The Sink
  assigns MOQ coordinates and finishes the namespace publication when it
  terminates.
- `Membrane.MOQX.TrackAdapter.ToTrack` and `FromTrack` are explicit filters for
  converting concrete Membrane formats to and from the canonical contract.

Authorization tokens remain explicit caller input and are passed to `moqx` as
redacted `MOQX.Secret` values.

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

Use `Membrane.MOQX.CatalogSource` when the pipeline should discover possible
tracks and decide which ones to attach. Catalog entries produce
`{:track_available, %Membrane.MOQX.TrackOffer{}}` parent notifications; they do
not modify the graph automatically. Linking an exact dynamic output pad starts
its one-track subscription.

Catalog membership is discovery, not admission. To request a track before it
is advertised, link a dynamic output pad with `track` and `stream_format`
options. This sends the subscription immediately and allows a remote
pull-based publisher to provision the track on demand.

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
The Sink does not encode, mux, split, or inspect media samples.

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
Dockerized relay checks remain explicitly selected integration tests.

### Live Cloudflare validation

Live relay tests are excluded from normal test runs. The Source test verifies
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

It defaults to Cloudflare's public draft-14 relay. Override `MOQX_ENDPOINT` for
another relay. For a managed relay, set `MOQX_AUTHORIZATION_FILE` to a file
containing the token; the test reads it into `MOQX.Secret` and never accepts the
token itself as an environment value.
