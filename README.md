# nanoprotocol

Shared WebSocket wire protocol for the MyriadMind ecosystem.

## Overview

`nanoprotocol` defines all message types, constants, and the client used by `nanobus`, `nanogateway`, `nanosec`, `nanorunner`, `nanoportal`, and `nanoauth`. It is a pure protocol library with zero business logic.

## Scope

**Contains:**
- `Message` envelope with typed payloads
- 70 message type constants organized by service boundary
- All request/response types for every protocol operation
- `MediaItem` for platform-agnostic media attachments
- `Client` — WebSocket client with auto-reconnect and RPC-style methods (plus HTTP/SSE helpers)
- `ConfigItem` — key-value configuration type

**Does NOT contain:**
- Business logic, database access, LLM calls, or chat platform SDKs

## Dependencies

Only `github.com/gorilla/websocket v1.5.3` and the Go standard library. No other imports are permitted.

## Message Types

### Gateway ↔ Bus

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeResolveUser` | Gateway → Bus | Resolve a platform identity to a user |
| `TypeUserResolved` | Bus → Gateway | Response with user_id, binding_id, is_new |
| `TypePostInbound` | Gateway → Bus | Post an incoming user message |
| `TypePollOutbound` | Gateway → Bus | Poll for pending outbound messages |
| `TypeOutbound` | Bus → Gateway | Response with an outbound message |

### Agent ↔ Bus

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypePollInbound` | Agent → Bus | Poll for pending inbound messages |
| `TypeInbound` | Bus → Agent | Response with an inbound message |
| `TypePostOutbound` | Agent → Bus | Post a response back to the user |

### Streaming

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeOutboundChunk` | Agent → Bus → Gateway | Incremental streaming chunk |
| `TypeOutboundComplete` | Agent → Bus → Gateway | Streaming turn finished |

### Container Management (Runner ↔ Bus)

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeRequestContainer` | Any → Bus | Request a container |
| `TypeContainerRequested` | Bus → Any | Confirmation of container request |
| `TypeListContainers` | Any → Bus | List containers for a user |
| `TypeContainersListed` | Bus → Any | Response with container list |
| `TypePollPendingContainers` | Runner → Bus | Poll for provisioning requests |
| `TypePendingContainers` | Bus → Runner | Response with pending requests |
| `TypeUpdateContainerStatus` | Runner → Bus | Report container status changes |
| `TypeDisposeContainer` | Any → Bus | Request container disposal |
| `TypeContainerDisposed` | Bus → Any | Confirmation of disposal |

### Auth (Service ↔ Bus ↔ nanoauth)

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeAuthenticate` | Service → Bus | Log in with email + password |
| `TypeAuthenticated` | Bus → Service | Response with JWT tokens |
| `TypeValidateToken` | Service → Bus | Validate a bearer token |
| `TypeTokenValid` | Bus → Service | Response: token valid / claims |
| `TypeRegisterAccount` | Service → Bus | Register a new account |
| `TypeAccountRegistered` | Bus → Service | Confirmation of registration |

### Agent Management (Portal ↔ Bus)

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeCreateAgent` | Portal → Bus | Create a per-user agent |
| `TypeAgentCreated` | Bus → Portal | Confirmation |
| `TypeListAgents` | Portal → Bus | List agents for a user |
| `TypeAgentsListed` | Bus → Portal | Response with agent records |
| `TypeUpdateAgent` | Portal → Bus | Update agent metadata |
| `TypeAgentUpdated` | Bus → Portal | Confirmation |
| `TypeDeleteAgent` | Portal → Bus | Delete an agent |
| `TypeAgentDeleted` | Bus → Portal | Confirmation |

### Agent-Scoped Config

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeGetAgentConfig` | Service → Bus | Get config items for an agent |
| `TypeAgentConfig` | Bus → Service | Response with config items |
| `TypeSetAgentConfig` | Service → Bus | Set an agent config item |
| `TypeAgentConfigSet` | Bus → Service | Confirmation |
| `TypeConfigChanged` | Bus → Service | SSE push of config changes |

### User Management

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeUpdateUser` | Any → Bus | Update a user record |
| `TypeUserUpdated` | Bus → Any | Confirmation |
| `TypeGetUser` | Any → Bus | Get a user record |
| `TypeUser` | Bus → Any | Response with user record |
| `TypeUpdateUserStatus` | Any → Bus | Change user status |
| `TypeGetUserInfo` | Any → Bus | Get user status and tier |
| `TypeUserInfo` | Bus → Any | Response with user details |

### Sessions

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeSaveSession` | Agent → Bus | Persist a session snapshot |
| `TypeSessionSaved` | Bus → Agent | Confirmation |
| `TypeArchiveSession` | Service → Bus | Archive a session |
| `TypeSessionArchived` | Bus → Service | Confirmation |
| `TypeListSessions` | Portal → Bus | List sessions for an agent |
| `TypeSessionsListed` | Bus → Portal | Response with session metadata |
| `TypeGetSession` | Portal → Bus | Get a full session |
| `TypeSession` | Bus → Portal | Response with session |

### Hypervisor Tunnel (Hypervisor ↔ Bus via WS)

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeTunnelRegister` | Hypervisor → Bus | Register a workspace tunnel |
| `TypeTunnelRegistered` | Bus → Hypervisor | Confirmation |
| `TypeTunnelRequest` | Bus ↔ Hypervisor | Tunnel request |
| `TypeTunnelResponse` | Bus ↔ Hypervisor | Tunnel response |

### Audit

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeGetAuditLog` | Portal → Bus | Query the audit trail |
| `TypeAuditLog` | Bus → Portal | Response with audit entries |

### WeChat KF Sync

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeGetWeChatKFSyncState` | Gateway → Bus | Get sync cursor and callback token |
| `TypeWeChatKFSyncState` | Bus → Gateway | Response with sync state |
| `TypeUpdateWeChatKFCursor` | Gateway → Bus | Persist sync cursor |
| `TypeUpdateWeChatKFCallbackTok` | Gateway → Bus | Persist callback token |

### Configuration

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeGetConfigItems` | Service → Bus | Get config key-value pairs |
| `TypeConfigItems` | Bus → Service | Response with config items |
| `TypeSetConfigItem` | Service → Bus | Set a single config item |
| `TypeConfigItemSet` | Bus → Service | Confirmation of config update |

### Runner Registry

| Constant | Direction | Purpose |
|----------|-----------|---------|
| `TypeRegisterRunner` | Runner → Bus | Register a new runner instance |
| `TypeRunnerRegistered` | Bus → Runner | Confirmation of registration |

### Error

| Constant | Purpose |
|----------|---------|
| `TypeError` | Generic error response |

## Client API

```go
client, err := nanoprotocol.NewClient("ws://localhost:9000")
defer client.Close()

// User resolution
resp, _ := client.ResolveUser("wecom", "user123")

// Post inbound message
client.PostInbound(nanoprotocol.InboundMessage{
    UserID:    "alice",
    BindingID: 1,
    AgentID:   "alice-default",
    SessionID: "wecom:group456",
    Channel:   "wecom",
    ChatID:    "group456",
    Content:   "Hello!",
})

// Poll for inbound messages (agent-side)
msg, _ := client.PollInbound("alice", 1, "agent-uuid")
if msg != nil {
    // process message
}

// Poll for outbound messages (gateway-side)
msg, _ := client.PollOutbound("wecom", "gateway-uuid")

// Auth (proxied to nanoauth by the bus)
tokens, _ := client.Authenticate("alice@example.com", "password")

// Agent management
records, _ := client.ListAgents("alice")

// Sessions
sessions, _ := client.ListSessions("alice-default", 20, 0)

// Config
items, _ := client.GetConfigItems("gateway")

// Config change notifications (SSE)
for change := range client.ConfigChanges() {
    // change is a ConfigChange
}
```

Key behaviors:
- `Send()` blocks on a single WebSocket read with a 30-second deadline
- Auto-reconnect on disconnection: `Send()` re-dials if connection is nil
- Read errors close the connection, forcing a fresh reconnect on the next call
- `Poll*` methods return `nil, nil` when no message is available
- HTTP REST + SSE helpers (`/api/v1/rpc`, `ConfigChanges()` via SSE) back the RPC methods

## Extending the Protocol

To add a new message type:

1. Add a `Type*` constant in `messages.go`
2. Define request/response structs in `messages.go`
3. Add a convenience method on `*Client` in `client.go`
4. Implement the handler in `nanobus/internal/server/`
5. Add store operations in `nanobus/internal/store/`

Protocol change workflow (tag + bump + push all submodules):

```bash
./scripts/bump-protocol.sh vX.Y.Z
```

## License

Mulan Permissive Software License, Version 2 (Mulan PSL v2)
