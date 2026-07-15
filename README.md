# Membrane MOQX Plugin

Membrane Framework Source and Sink elements for receiving and publishing
Media over QUIC through [`moqx`](https://github.com/dmorn/moqx).

The initial integration targets Cloudflare's deployed MOQT draft-14 relays over
native QUIC. `moqx` owns the transport and protocol lifecycle; this project
adapts its typed subscriptions, publications, objects, and terminal events to
Membrane elements and buffers.

## Status

`Membrane.MOQX.Sink` publishes already-packaged CMAF tracks through the public
`moqx` API. The Source remains under development.

The first version will rely on Membrane's Toilet for pipeline overload handling.
Protocol-neutral demand credits and bounded delivery inside `moqx` are deferred
until real pipeline usage demonstrates that they are needed.

## Media boundary

MOQT transports media objects; it does not define raw codec framing. The first
elements will carry CMAF initialization and media fragments without decoding or
encoding H.264. Pipelines that need frame-level H.264 buffers should compose the
appropriate Membrane CMAF/MP4 muxer or demuxer around this plugin.

## Elements

- `Membrane.MOQX.Source` connects to a relay, subscribes to a selected track,
  and converts received MOQ objects and lifecycle events into Membrane output
  (planned).
- `Membrane.MOQX.Sink` advertises a namespace and a retained full catalog. Each
  requested input pad is one already-packaged logical track. The Sink assigns
  MOQ coordinates and finishes the namespace publication when it terminates.

Authorization tokens remain explicit caller input and are passed to `moqx` as
redacted `MOQX.Secret` values.

## Publishing CMAF

The Sink accepts `Membrane.CMAF.Track` stream formats containing AAC or H.264.
One input buffer becomes one MOQ object without changing its payload.
`metadata.last_chunk?` controls segment boundaries; when absent, the buffer is
treated as a complete segment.

```elixir
import Membrane.ChildrenSpec

alias Membrane.MOQX.Sink
alias Membrane.Pad

require Pad

source
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
`Membrane.MOQX.TrackAdapter` behaviour. `describe/2` returns a
`Membrane.MOQX.TrackDescriptor`; `publication_unit/2` returns exactly one
`Membrane.MOQX.PublicationUnit` with an unchanged binary payload. Select the
module explicitly on the pad:

```elixir
via_in(Pad.ref(:input, :custom),
  options: [
    track_name: "custom.media",
    adapter: MyApp.MOQXTrackAdapter
  ]
)
```

Adapters describe packaging and publication boundaries only. The Sink remains
the owner of MOQ coordinates, catalog revisions, MOQX operations, and
lifecycle handling. Adapter selection is local to the pad; there is no global
registry or Application-environment lookup.

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

The live Sink roundtrip is excluded from normal test runs. Give it a real
fragmented H.264 MP4 fixture and select the integration test explicitly:

```bash
MOQX_CMAF_FIXTURE=/tmp/input-fragmented.mp4 \
  mix test --only integration test/integration/cloudflare_sink_test.exs
```

It defaults to Cloudflare's public draft-14 relay. Override `MOQX_ENDPOINT` for
another relay. For a managed relay, set `MOQX_AUTHORIZATION_FILE` to a file
containing the token; the test reads it into `MOQX.Secret` and never accepts the
token itself as an environment value.
