# nanoprotocol

The wire contract every service compiles against: message types, the envelope, the
clients, and the permission matcher.

It is a **Go module, not a service** — no port, no database, no side effects.

## What it is

- **81 message type constants**, grouped by the boundary they cross: gateway ↔ bus,
  agent ↔ bus, container management, auth, streaming, the agent socket, the tunnel
  and terminal streams.
- **One envelope.** `Message` carries the type, the payload and the routing fields;
  every request has a matching response type.
- **Two clients.** A WebSocket client with reconnect and RPC-style calls, and an
  HTTP client for the REST and SSE surfaces.
- **Shared semantics that must not drift.** `MediaItem`, `ConfigItem`, the session
  envelope, and `WildcardMatch` — the one implementation of permission patterns that
  every service uses when it decides whether a tool may run.
- **No business logic.** No database, no LLM, no chat SDK, no configuration loading.

## Where it sits

| | |
|---|---|
| Kind | Library — imported by ten modules |
| Version | `0.46.0` in the release; pinned by the super-repo's `versions.lock` |
| Dependencies | `github.com/gorilla/websocket` and the standard library only |
| Consumers | `nanobus`, `nanogateway`, `nanoagent`, `nanoportal`, `nanoauth`, `nanoadmin`, `nanoprovisioner`, `nanoartifacts`, `nanoworkspace`, `nanoui` |

## Quick start

```bash
go get gitee.com/kobeyoung81/nanoprotocol@v0.46.0
```

```go
c := nanoprotocol.NewClient(busURL)
msg := nanoprotocol.Message{Type: nanoprotocol.TypePollInbound, UserID: userID}
resp, err := c.Request(ctx, msg)
if err == nil && resp.Type == nanoprotocol.TypeInbound {
    // resp.Inbound carries the message
}
```

Inside the workspace the module is used through `go.work`; a consumer never vendors
it, so a protocol change is one tag.

## Message groups

| Group | Boundary | Notes |
|---|---|---|
| Gateway | gateway ↔ bus | resolve a platform identity, post inbound, poll outbound |
| Agent | agent ↔ bus | poll inbound, post outbound, stream chunks and completion |
| Container | runner ↔ bus | request, list, poll pending, update status, dispose |
| Auth | service ↔ bus ↔ nanoauth | login, register, validate a token |
| Config | service ↔ bus | agent-scoped configuration and change events |
| Users | any ↔ bus | update, status, info |
| Sessions | agent / console ↔ bus | save, archive, list, get |
| Tunnel | hypervisor ↔ bus | register, request, response |
| Terminal | browser ↔ bus ↔ hypervisor | attach, data, close |
| Agent socket | agent ↔ bus (WS) | the same calls as `/api/v1/rpc`, plus two stateless pushes |

**Agent management is not in the protocol.** Creating, editing, listing and deleting
an agent is an HTTP call to the portal, which owns the rows; the only thing left on
the bus is the change notification a supervisor subscribes to.

## Design decisions

- **Types are strings, not numbers.** A log line or an SSE frame is readable, and an
  unknown type can be reported instead of silently mis-decoded.
- **One dependency.** `gorilla/websocket` — anything else would leak a transport or a
  framework into every service.
- **The permission matcher lives here.** Every module has to answer "may this tool
  run?" the same way; one implementation is the only way that stays true.
- **The envelope is stable, the payloads grow.** New features add fields and types;
  they do not change the shape a consumer already decodes.
- **No version negotiation at runtime.** A mismatched protocol is caught at build
  time, which is why every consumer pins the same tag.

## Changing the protocol

1. Commit the change here.
2. Run `./scripts/bump-protocol.sh vX.Y.Z` in the super-repo — it tags this module and
   updates every consumer's `go.mod`.
3. Commit the bumped `go.mod` / `go.sum` in each consumer.
4. Push the submodules **before** the super-repo, so a checkout never points at a tag
   that is not on the remote yet.

## Docs

- [`docs/ARCHITECTURE.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/docs/ARCHITECTURE.md) — how the services use this contract
- [`docs/README.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/docs/README.md) — every document

## Status

Released as `v0.46.0`; the version is pinned in the super-repo's `versions.lock`.

## Contributing and security

[`CONTRIBUTING.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/CONTRIBUTING.md)
for setup and commit rules; report vulnerabilities privately through
[`SECURITY.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/SECURITY.md), never
in a public issue.

## License

[AGPL-3.0](./LICENSE). The name and logo are covered separately — see
[`NOTICE`](./NOTICE).
