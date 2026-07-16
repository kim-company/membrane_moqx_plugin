# Membrane MOQX Plugin

Membrane Framework Source and Sink elements for receiving and publishing
arbitrary named object tracks through [`moqx`](https://github.com/dmorn/moqx).

The initial integration targets Cloudflare's deployed MOQT draft-14 relays over
native QUIC. `moqx` owns the transport and protocol lifecycle; this project
adapts its typed subscriptions, publications, objects, and terminal events to
Membrane elements and buffers.

## Status

`Membrane.MOQX.Sink` publishes the canonical `Membrane.MOQX.Track` format
through the public `moqx` API. Explicit format adapters convert CMAF and custom
Membrane formats at neighboring pipeline boundaries. The Source remains under
development.

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

- `Membrane.MOQX.Source` connects to a relay, subscribes to a selected track,
  and converts received MOQ objects and lifecycle events into Membrane output
  (planned).
- `Membrane.MOQX.Sink` advertises a namespace and a retained full catalog. Each
  requested input pad accepts one canonical `Membrane.MOQX.Track`. The Sink
  assigns MOQ coordinates and finishes the namespace publication when it
  terminates.
- `Membrane.MOQX.TrackAdapter.ToTrack` and `FromTrack` are explicit filters for
  converting concrete Membrane formats to and from the canonical contract.

Authorization tokens remain explicit caller input and are passed to `moqx` as
redacted `MOQX.Secret` values.

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
mise exec -- mix deps.get
mise exec -- mix test
mise exec -- mix format --check-formatted
mise exec -- mix compile --warnings-as-errors
mise exec -- mix credo --strict
```

Hermetic element tests should use `MOQX.Testing.Transport`. Live Cloudflare and
Dockerized relay checks remain explicitly selected integration tests.

### Live Cloudflare validation

The live Sink roundtrip is excluded from normal test runs. Give it a real
fragmented H.264 MP4 fixture and select the integration test explicitly:

```bash
MOQX_CMAF_FIXTURE=/tmp/input-fragmented.mp4 \
  mise exec -- mix test --only integration test/integration/cloudflare_sink_test.exs
```

It defaults to Cloudflare's public draft-14 relay. Override `MOQX_ENDPOINT` for
another relay. For a managed relay, set `MOQX_AUTHORIZATION_FILE` to a file
containing the token; the test reads it into `MOQX.Secret` and never accepts the
token itself as an environment value.
