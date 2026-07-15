# Membrane MOQX Plugin

Membrane Framework Source and Sink elements for receiving and publishing
Media over QUIC through [`moqx`](https://github.com/dmorn/moqx).

The initial integration targets Cloudflare's deployed MOQT draft-14 relays over
native QUIC. `moqx` owns the transport and protocol lifecycle; this project
adapts its typed subscriptions, publications, objects, and terminal events to
Membrane elements and buffers.

## Status

This project is under initial development. The first milestone is a usable
Source and Sink pair backed by the public `moqx` API.

The first version will rely on Membrane's Toilet for pipeline overload handling.
Protocol-neutral demand credits and bounded delivery inside `moqx` are deferred
until real pipeline usage demonstrates that they are needed.

## Media boundary

MOQT transports media objects; it does not define raw codec framing. The first
elements will carry CMAF initialization and media fragments without decoding or
encoding H.264. Pipelines that need frame-level H.264 buffers should compose the
appropriate Membrane CMAF/MP4 muxer or demuxer around this plugin.

## Planned elements

- `Membrane.MOQX.Source` connects to a relay, subscribes to a selected track,
  and converts received MOQ objects and lifecycle events into Membrane output.
- `Membrane.MOQX.Sink` advertises a namespace and track, converts incoming CMAF
  buffers into MOQ objects, and finishes the publication when the input ends.

Authorization tokens remain explicit caller input and are passed to `moqx` as
redacted `MOQX.Secret` values.

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
```

Hermetic element tests should use `MOQX.Testing.Transport`. Live Cloudflare and
Dockerized relay checks remain explicitly selected integration tests.
