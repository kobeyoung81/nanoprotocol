// Package nanoprotocol defines the shared WebSocket message types used for
// communication between the bus, gateway, and agent services.
package nanoprotocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Message type constants.
const (
	// Gateway → Bus
	TypeResolveUser  = "resolve_user"
	TypePostInbound  = "post_inbound"
	TypePollOutbound = "poll_outbound"

	// Bus → Gateway
	TypeUserResolved = "user_resolved"
	TypeOutbound     = "outbound"

	// Agent → Bus
	TypePollInbound  = "poll_inbound"
	TypePostOutbound = "post_outbound"

	// Bus → Agent
	TypeInbound = "inbound"

	// Container management (Runner <-> Bus)
	TypeRequestContainer      = "request_container"
	TypeContainerRequested    = "container_requested"
	TypeListContainers        = "list_containers"
	TypeContainersListed      = "containers_listed"
	TypePollPendingContainers = "poll_pending_containers"
	TypePendingContainers     = "pending_containers"
	TypeUpdateContainerStatus = "update_container_status"
	TypeDisposeContainer      = "dispose_container"
	TypeContainerDisposed     = "container_disposed"

	// Auth (Gateway/Nanosec <-> Bus <-> nanoauth)
	TypeAuthenticate      = "authenticate"
	TypeAuthenticated     = "authenticated"
	TypeValidateToken     = "validate_token"
	TypeTokenValid        = "token_valid"
	TypeRegisterAccount   = "register_account"
	TypeAccountRegistered = "account_registered"

	// Agent management (Portal <-> Bus)
	TypeCreateAgent  = "create_agent"
	TypeAgentCreated = "agent_created"
	TypeListAgents   = "list_agents"
	TypeAgentsListed = "agents_listed"
	TypeUpdateAgent  = "update_agent"
	TypeAgentUpdated = "agent_updated"
	TypeDeleteAgent  = "delete_agent"
	TypeAgentDeleted = "agent_deleted"

	// Agent runtime status (Hypervisor <-> Bus)
	TypeUpdateAgentStatus = "update_agent_status"

	// Agent change notification (Bus -> Hypervisor, SSE event type)
	TypeAgentsChanged = "agents_changed"

	// Agent-scoped config
	TypeGetAgentConfig = "get_agent_config"
	TypeAgentConfig    = "agent_config"
	TypeSetAgentConfig = "set_agent_config"
	TypeAgentConfigSet = "agent_config_set"
	TypeConfigChanged  = "config_changed"

	// User management
	TypeUpdateUser  = "update_user"
	TypeUserUpdated = "user_updated"
	TypeGetUser     = "get_user"
	TypeUser        = "user"

	// Streaming (Nanosec → Bus → Gateway)
	TypeOutboundChunk    = "outbound_chunk"
	TypeOutboundComplete = "outbound_complete"

	// Hypervisor tunnel (Hypervisor <-> Bus via WS)
	TypeTunnelRegister   = "tunnel_register"
	TypeTunnelRegistered = "tunnel_registered"
	TypeTunnelRequest    = "tunnel_request"
	TypeTunnelResponse   = "tunnel_response"

	// Hypervisor tunnel terminal streams (Byte-channel + control over the
	// single per-user hypervisor WebSocket; Browser <-> Bus <-> Hypervisor)
	TypeTunnelStreamOpen   = "tunnel_stream_open"
	TypeTunnelStreamInput  = "tunnel_stream_input"
	TypeTunnelStreamResize = "tunnel_stream_resize"
	TypeTunnelStreamClose  = "tunnel_stream_close"
	TypeTunnelStreamOutput = "tunnel_stream_output"
	TypeTunnelStreamClosed = "tunnel_stream_closed"

	// Session access (for web portal)
	TypeListSessions   = "list_sessions"
	TypeSessionsListed = "sessions_listed"
	TypeGetSession     = "get_session"
	TypeSession        = "session"

	// Session persistence (Nanosec → Bus → MySQL)
	TypeSaveSession     = "save_session"
	TypeSessionSaved    = "session_saved"
	TypeArchiveSession  = "archive_session"
	TypeSessionArchived = "session_archived"

	// Audit
	TypeGetAuditLog = "get_audit_log"
	TypeAuditLog    = "audit_log"

	// User management
	TypeUpdateUserStatus = "update_user_status"
	TypeGetUserInfo      = "get_user_info"
	TypeUserInfo         = "user_info"

	// WeChat KF sync state
	TypeGetWeChatKFSyncState      = "get_wechatkf_sync_state"
	TypeWeChatKFSyncState         = "wechatkf_sync_state"
	TypeUpdateWeChatKFCursor      = "update_wechatkf_cursor"
	TypeUpdateWeChatKFCallbackTok = "update_wechatkf_callback_token"

	// Configuration management (Any component → Bus)
	TypeGetConfigItems = "get_config_items"
	TypeConfigItems    = "config_items"
	TypeSetConfigItem  = "set_config_item"
	TypeConfigItemSet  = "config_item_set"

	// Agent capabilities (Nanosec → Bus): catalog fetch + builtin sync
	TypeGetCapabilities  = "get_capabilities"
	TypeCapabilitiesResp = "capabilities_response"
	TypeBuiltinToolsSync = "builtin_tools_sync"

	// Consolidated agent snapshot (Nanosec → Bus): record + config +
	// capabilities (+ resolved models) in one round trip for the reconciler.
	TypeGetAgentSnapshot = "get_agent_snapshot"
	TypeAgentSnapshot    = "agent_snapshot"

	// Agent types & permission keys (Portal/Agent → Bus): platform policy plane.
	TypeListAgentTypes     = "list_agent_types"
	TypeAgentTypes         = "agent_types"
	TypeGetAgentType       = "get_agent_type"
	TypeSetAgentType       = "set_agent_type"
	TypeAgentTypeSet       = "agent_type_set"
	TypeListPermissionKeys = "list_permission_keys"
	TypePermissionKeys     = "permission_keys"

	// Per-agent permission override (Nanosec → Bus): persists an "always allow"
	// (or deny) rule on a capability binding.
	TypeSetAgentPermission = "set_agent_permission"
	TypeAgentPermissionSet = "agent_permission_set"

	// Skill authoring / acknowledgement (Nanosec → Bus).
	TypeSkillSave    = "skill_save"
	TypeSkillSaved   = "skill_saved"
	TypeSkillDelete  = "skill_delete"
	TypeSkillDeleted = "skill_deleted"
	TypeSkillAck     = "skill_ack"
	TypeSkillAcked   = "skill_acked"

	// Binding agent preferences (Gateway/Portal → Bus): which agent a channel
	// binding (channel + sender) talks to by default.
	TypeGetBindingAgent = "get_binding_agent"
	TypeBindingAgent    = "binding_agent"
	TypeSetBindingAgent = "set_binding_agent"
	TypeBindingAgentSet = "binding_agent_set"

	// Runner registry (Runner → Bus)
	TypeRegisterRunner   = "register_runner"
	TypeRunnerRegistered = "runner_registered"

	// Errors
	TypeError = "error"
)

// Message is the WebSocket message envelope exchanged between services.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// NewMessage creates a Message with the given type and payload struct.
// The payload is marshaled to JSON. Returns an error if marshaling fails.
func NewMessage(msgType string, payload any) (Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("marshal payload: %w", err)
	}
	return Message{
		Type:    msgType,
		Payload: data,
	}, nil
}

// NewErrorMessage creates an error Message.
func NewErrorMessage(errMsg string) Message {
	return Message{
		Type:  TypeError,
		Error: errMsg,
	}
}

// DecodePayload unmarshals the message payload into the given target.
func (m *Message) DecodePayload(target any) error {
	if m.Payload == nil {
		return fmt.Errorf("empty payload")
	}
	return json.Unmarshal(m.Payload, target)
}

// ResolveUserRequest is sent by gateway to resolve a platform identity to a user ID.
type ResolveUserRequest struct {
	Channel  string `json:"channel"`
	SenderID string `json:"sender_id"`
}

// ResolveUserResponse is the bus reply with the resolved user ID.
type ResolveUserResponse struct {
	UserID    string `json:"user_id"`
	BindingID int64  `json:"binding_id,omitempty"`
	IsNew     bool   `json:"is_new"`
	Status    string `json:"status"`
}

// MediaItem describes a media attachment associated with a message.
type MediaItem struct {
	Platform string `json:"platform,omitempty"` // "wecom", "wechatkf", "feishu", "local"
	MediaID  string `json:"media_id,omitempty"`
	Filename string `json:"filename,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	URL      string `json:"url,omitempty"` // file:///data/media/{hash} or platform URL
}

// InboundMessage represents a message from a user, routed from gateway → bus → agent.
type InboundMessage struct {
	UserID    string         `json:"user_id"`
	BindingID int64          `json:"binding_id,omitempty"`
	AgentID   string         `json:"agent_id,omitempty"`
	SessionID string         `json:"session_id"`
	Channel   string         `json:"channel"`
	ChatID    string         `json:"chat_id"`
	SenderID  string         `json:"sender_id"`
	MessageID string         `json:"message_id,omitempty"`
	Content   string         `json:"content"`
	Media     []MediaItem    `json:"media,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

// OutboundMessage represents a response from an agent, routed from agent → bus → gateway.
type OutboundMessage struct {
	UserID    string         `json:"user_id"`
	BindingID int64          `json:"binding_id,omitempty"`
	AgentID   string         `json:"agent_id,omitempty"`
	SessionID string         `json:"session_id"`
	Channel   string         `json:"channel"`
	ChatID    string         `json:"chat_id"`
	MessageID string         `json:"message_id,omitempty"`
	Content   string         `json:"content"`
	Media     []MediaItem    `json:"media,omitempty"`
	ReplyTo   string         `json:"reply_to,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// PollInboundRequest is sent by an agent to poll for pending messages.
type PollInboundRequest struct {
	UserID    string `json:"user_id"`
	BindingID int64  `json:"binding_id,omitempty"`
	AgentID   string `json:"agent_id"`
}

// PollOutboundRequest is sent by a gateway to poll for outbound messages.
type PollOutboundRequest struct {
	Channel   string `json:"channel"`
	GatewayID string `json:"gateway_id"`
}

// ContainerRequest describes a pending container provisioning request.
type ContainerRequest struct {
	UserID    string `json:"user_id"`
	BindingID int64  `json:"binding_id,omitempty"`
	AgentID   string `json:"agent_id,omitempty"`
	Level     string `json:"level"`
	Status    string `json:"status"`
	// DisplayName is the account display name the container runs as. It is
	// derived by the bus (users table) and used by the runner to name the
	// container's Linux user and data home.
	DisplayName string `json:"display_name,omitempty"`
}

// RequestContainerRequest asks the bus to enqueue a container provisioning request.
type RequestContainerRequest struct {
	UserID    string `json:"user_id"`
	BindingID int64  `json:"binding_id,omitempty"`
	AgentID   string `json:"agent_id,omitempty"`
	Level     string `json:"level"`
}

// ListContainersRequest asks the bus for a user's container requests.
type ListContainersRequest struct {
	UserID string `json:"user_id"`
	Limit  int    `json:"limit,omitempty"`
}

// ListContainersResponse is the bus reply with the user's container requests.
type ListContainersResponse struct {
	Requests []ContainerRequest `json:"requests"`
}

// PollPendingContainersRequest is sent by a runner to poll for pending container requests.
type PollPendingContainersRequest struct {
	RunnerID string `json:"runner_id"`
}

// PollPendingContainersResponse is the bus reply with pending container requests.
type PollPendingContainersResponse struct {
	Requests []ContainerRequest `json:"requests"`
}

// UpdateContainerStatusRequest is sent by a runner to update a container's status.
type UpdateContainerStatusRequest struct {
	UserID    string `json:"user_id,omitempty"`
	BindingID int64  `json:"binding_id,omitempty"`
	Status    string `json:"status"`
}

// DisposeContainerRequest asks the bus to enqueue a container disposal request.
type DisposeContainerRequest struct {
	UserID    string `json:"user_id"`
	BindingID int64  `json:"binding_id,omitempty"`
	AgentID   string `json:"agent_id,omitempty"`
}

// UpdateUserStatusRequest is sent to update a user's status.
type UpdateUserStatusRequest struct {
	UserID string `json:"user_id"`
	Status string `json:"status"`
}

// GetUserInfoRequest is sent to get user information.
type GetUserInfoRequest struct {
	UserID string `json:"user_id"`
}

// UserInfoResponse is the bus reply with user information.
type UserInfoResponse struct {
	UserID string `json:"user_id"`
	Status string `json:"status"`
	Level  string `json:"level"`
}

// GetWeChatKFSyncStateRequest asks the bus for the current sync state of a WeChat KF account.
type GetWeChatKFSyncStateRequest struct {
	OpenKfID string `json:"open_kfid"`
}

// WeChatKFSyncStateResponse contains the persisted sync state for a WeChat KF account.
type WeChatKFSyncStateResponse struct {
	Found          bool      `json:"found"`
	OpenKfID       string    `json:"open_kfid,omitempty"`
	Cursor         string    `json:"cursor,omitempty"`
	CallbackToken  string    `json:"callback_token,omitempty"`
	TokenExpiresAt time.Time `json:"token_expires_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

// UpdateWeChatKFCursorRequest updates the persisted cursor for a WeChat KF account.
type UpdateWeChatKFCursorRequest struct {
	OpenKfID string `json:"open_kfid"`
	Cursor   string `json:"cursor"`
}

// UpdateWeChatKFCallbackTokenRequest updates the short-lived callback token for a WeChat KF account.
type UpdateWeChatKFCallbackTokenRequest struct {
	OpenKfID       string    `json:"open_kfid"`
	CallbackToken  string    `json:"callback_token"`
	TokenExpiresAt time.Time `json:"token_expires_at"`
}

// ConfigItem represents a single configuration key-value pair with metadata.
type ConfigItem struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	ValueType   string `json:"value_type"` // "string", "int", "bool", "duration", "secret"
	Description string `json:"description"`
}

// GetConfigItemsRequest is sent by a component to retrieve its configuration items from the bus.
type GetConfigItemsRequest struct {
	ComponentType string `json:"component_type"` // "bus", "gateway", "runner", "agent"
}

// GetConfigItemsResponse is the bus reply with the component's configuration items.
type GetConfigItemsResponse struct {
	ComponentType string       `json:"component_type"`
	Items         []ConfigItem `json:"items"`
}

// SetConfigItemRequest is sent to update a single configuration item.
type SetConfigItemRequest struct {
	ComponentType string `json:"component_type"`
	Key           string `json:"key"`
	Value         string `json:"value"`
}

// SetConfigItemResponse confirms the updated configuration item.
type SetConfigItemResponse struct {
	ComponentType string `json:"component_type"`
	Key           string `json:"key"`
	Value         string `json:"value"`
}

// RegisterRunnerRequest is sent by a runner to register itself with the bus.
type RegisterRunnerRequest struct {
	RunnerID   string `json:"runner_id"`
	Hostname   string `json:"hostname"`
	DockerHost string `json:"docker_host"`
}

// RegisterRunnerResponse is the bus reply confirming runner registration.
type RegisterRunnerResponse struct {
	RunnerID string `json:"runner_id"`
}

// ======== Auth Types ========

type AuthenticateRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthenticateResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"`
}

type ValidateTokenRequest struct {
	Token string `json:"token"`
}

type ValidateTokenResponse struct {
	Valid  bool   `json:"valid"`
	UserID string `json:"user_id,omitempty"`
	Role   string `json:"role,omitempty"`
}

type RegisterAccountRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name,omitempty"`
}

type RegisterAccountResponse struct {
	UserID string `json:"user_id"`
}

// ======== Agent Management Types ========

type CreateAgentRequest struct {
	UserID       string `json:"user_id"`
	ID           string `json:"id"`
	TypeID       string `json:"type_id,omitempty"`
	DisplayName  string `json:"display_name"`
	Description  string `json:"description,omitempty"`
	SystemPrompt string `json:"system_prompt,omitempty"`
	Model        string `json:"model,omitempty"`
	ModelID      int64  `json:"model_id,omitempty"`
	// GroupID is the user's grouping for this agent ("" = ungrouped). Groups are
	// private to their owner; the bus rejects an id that does not belong to the
	// creating user.
	GroupID string `json:"group_id,omitempty"`
}

type AgentInfo struct {
	ID             string           `json:"id"`
	UserID         string           `json:"user_id"`
	TypeID         string           `json:"type_id,omitempty"`
	GroupID        string           `json:"group_id,omitempty"`
	SessionAgents  []string         `json:"session_agents,omitempty"`
	DefaultAgent   string           `json:"default_agent,omitempty"`
	DisplayName    string           `json:"display_name"`
	Description    string           `json:"description"`
	SystemPrompt   string           `json:"system_prompt"`
	Model          string           `json:"model"`
	ModelID        int64            `json:"model_id,omitempty"`
	Thinking       string           `json:"thinking,omitempty"`    // "" = follow model; off|low|medium|high
	McpEnabled     *bool            `json:"mcp_enabled,omitempty"` // nil = default (enabled)
	TitleModel     string           `json:"title_model,omitempty"`
	SummarizeModel string           `json:"summarize_model,omitempty"`
	SubagentModel  string           `json:"subagent_model,omitempty"`
	Permissions    []PermissionRule `json:"permissions,omitempty"`
	// Settings are this agent's per-key runtime overrides (blank = inherit the
	// AgentType / code default). See AgentTypeDef.Settings for the key set.
	Settings      map[string]string `json:"settings,omitempty"`
	MaxIterations int               `json:"max_iterations,omitempty"`
	Status        string            `json:"status"`
	Tools         []string          `json:"tools"`
	Workspaces    []string          `json:"workspaces"`
	CreatedAt     string            `json:"created_at"`
}

type ListAgentsRequest struct {
	UserID string `json:"user_id"`
}

type ListAgentsResponse struct {
	Agents []AgentInfo `json:"agents"`
}

type UpdateAgentRequest struct {
	ID           string  `json:"id"`
	TypeID       *string `json:"type_id,omitempty"`
	DefaultAgent *string `json:"default_agent,omitempty"`
	DisplayName  *string `json:"display_name,omitempty"`
	Description  *string `json:"description,omitempty"`
	// GroupID moves the agent to another group when non-nil; an empty string
	// takes it out of every group (ungrouped). Absent leaves the grouping alone.
	GroupID        *string          `json:"group_id,omitempty"`
	SystemPrompt   *string          `json:"system_prompt,omitempty"`
	Model          *string          `json:"model,omitempty"`
	ModelID        *int64           `json:"model_id,omitempty"`
	Thinking       *string          `json:"thinking,omitempty"`
	McpEnabled     *bool            `json:"mcp_enabled,omitempty"`
	TitleModel     *string          `json:"title_model,omitempty"`
	SummarizeModel *string          `json:"summarize_model,omitempty"`
	SubagentModel  *string          `json:"subagent_model,omitempty"`
	Permissions    []PermissionRule `json:"permissions,omitempty"`
	// Settings replaces this agent's runtime override map when present (an
	// empty object clears all overrides). Absent leaves overrides unchanged.
	Settings     *map[string]string `json:"settings,omitempty"`
	ClearModelID bool               `json:"clear_model_id,omitempty"` // explicit reset to "no catalog model"
}

type DeleteAgentRequest struct {
	ID string `json:"id"`
}

type DeleteAgentResponse struct {
	ID string `json:"id"`
}

// UpdateAgentStatusRequest reports an agent's runtime status to the bus.
// Status is one of: spawning, running, failed, stopped.
type UpdateAgentStatusRequest struct {
	UserID  string `json:"user_id"`
	AgentID string `json:"agent_id"`
	Status  string `json:"status"`
}

// AgentChangeNotification is broadcast by the bus (SSE) when an agent is
// created, updated, or deleted so supervisors can react without a restart.
type AgentChangeNotification struct {
	UserID  string `json:"user_id"`
	Action  string `json:"action"` // create | update | delete
	AgentID string `json:"agent_id"`
}

// ======== User Management Types ========

type UpdateUserRequest struct {
	UserID      string  `json:"user_id"`
	DisplayName *string `json:"display_name,omitempty"`
	Role        *string `json:"role,omitempty"`
	Tier        *string `json:"tier,omitempty"`
	Status      *string `json:"status,omitempty"`
}

type UpdateUserResponse struct {
	UserID string `json:"user_id"`
}

type GetUserRequest struct {
	UserID string `json:"user_id"`
}

type GetUserResponse struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Role        string `json:"role"`
	Tier        string `json:"tier"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
}

// ======== Config Types ========

type GetAgentConfigRequest struct {
	AgentID string `json:"agent_id"`
}

type GetAgentConfigResponse struct {
	Items []ConfigItem `json:"items"`
}

type SetAgentConfigRequest struct {
	AgentID string `json:"agent_id"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

type SetAgentConfigResponse struct {
	AgentID string `json:"agent_id"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

type ConfigChangedNotification struct {
	AgentID string   `json:"agent_id"`
	Keys    []string `json:"keys"`
}

// ======== Capability Catalog Types ========

// CapabilityKind enumerates the catalog types an agent can be given.
type CapabilityKind string

const (
	CapabilityKindBuiltinTool CapabilityKind = "builtin_tool"
	CapabilityKindSkill       CapabilityKind = "skill"
	CapabilityKindMcp         CapabilityKind = "mcp"
)

// McpServerDef is a catalog entry for an MCP server.
type McpServerDef struct {
	ID             string            `json:"id"`
	Scope          string            `json:"scope"` // global | user
	OwnerUserID    string            `json:"owner_user_id,omitempty"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Transport      string            `json:"transport"` // http | sse
	URL            string            `json:"url,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	DefaultEnabled bool              `json:"default_enabled"`
	Locked         string            `json:"locked"` // "" | on | off
}

// SkillDef is a catalog entry for a skill (packaged SKILL.md files).
type SkillDef struct {
	ID             string            `json:"id"`
	Scope          string            `json:"scope"`
	OwnerUserID    string            `json:"owner_user_id,omitempty"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Files          map[string]string `json:"files"`
	DefaultEnabled bool              `json:"default_enabled"`
	Locked         string            `json:"locked"`
	// Versioning / provenance.
	Origin           string `json:"origin,omitempty"` // platform | human | agent
	Version          int    `json:"version,omitempty"`
	Status           string `json:"status,omitempty"` // draft | published
	PublishedVersion int    `json:"published_version,omitempty"`
	ContentHash      string `json:"content_hash,omitempty"`
	CreatedBy        string `json:"created_by,omitempty"`
	// UpdateAvailable is set on agent-facing resolution when a newer published
	// platform version exists than the user has acknowledged.
	UpdateAvailable bool `json:"update_available,omitempty"`
	AckedVersion    int  `json:"acked_version,omitempty"`
}

// BuiltinToolDef carries the runtime definition of one builtin tool so the bus
// catalog can self-heal from the agent binary.
type BuiltinToolDef struct {
	ID             string          `json:"id"`
	Description    string          `json:"description,omitempty"`
	Parameters     json.RawMessage `json:"parameters,omitempty"`
	DefaultEnabled bool            `json:"default_enabled"`
	Locked         string          `json:"locked"`
}

// CapabilityAttachment is one agent's per-capability override row.
type CapabilityAttachment struct {
	Kind    CapabilityKind `json:"kind"`
	Ref     string         `json:"ref"`
	Enabled bool           `json:"enabled"`
}

// GetCapabilitiesRequest fetches the capability catalog scoped to an agent.
type GetCapabilitiesRequest struct {
	AgentID string `json:"agent_id"`
}

// CapabilitiesResponse is the agent-facing catalog snapshot.
type CapabilitiesResponse struct {
	BuiltinTools []BuiltinToolDef       `json:"builtin_tools"`
	McpServers   []McpServerDef         `json:"mcp_servers"`
	Skills       []SkillDef             `json:"skills"`
	Attachments  []CapabilityAttachment `json:"attachments"`
	// Models is the model catalog visible to this agent's owner: platform
	// models plus the owner's own user-scoped models, credentials included so
	// the agent can construct per-model provider clients.
	Models []ModelDef `json:"models"`
	// AgentType is the policy blueprint the agent belongs to (nil if the agent
	// predates AgentTypes and has no type_id).
	AgentType *AgentTypeDef `json:"agent_type,omitempty"`
	// Permissions are the resolved (type-then-instance) permission rules.
	Permissions []PermissionRule `json:"permissions,omitempty"`
	// AgentPermissions are the raw instance-level override rules only (without
	// the type baseline). Clients editing per-agent permissions should load and
	// save this set, never the resolved one.
	AgentPermissions []PermissionRule `json:"agent_permissions,omitempty"`
}

// ======== AgentType / Permission Types ========

// PermissionAction is the gating decision for a permission rule.
type PermissionAction string

const (
	PermissionAllow PermissionAction = "allow"
	PermissionAsk   PermissionAction = "ask"
	PermissionDeny  PermissionAction = "deny"
)

// PermissionRule matches a tool-name pattern (wildcard "*") and returns an
// action. Rules are evaluated in order; the last match wins.
type PermissionRule struct {
	Pattern string           `json:"pattern"`
	Action  PermissionAction `json:"action"`
}

// PermissionKeyDef is an admin-authored entry in the permission key catalog.
type PermissionKeyDef struct {
	ID            string           `json:"id"`
	Description   string           `json:"description,omitempty"`
	DefaultAction PermissionAction `json:"default_action"`
	ReadOnly      bool             `json:"read_only"`
}

// TypeModelBinding is one allowed model on an AgentType.
type TypeModelBinding struct {
	ModelID   int64 `json:"model_id"`
	IsDefault bool  `json:"is_default"`
	Locked    bool  `json:"locked"`
}

// TypeCapabilityBinding is one capability baseline on an AgentType.
type TypeCapabilityBinding struct {
	Kind           CapabilityKind   `json:"kind"`
	Ref            string           `json:"ref"`
	DefaultEnabled bool             `json:"default_enabled"`
	Locked         string           `json:"locked"` // "" | on | off
	Params         string           `json:"params,omitempty"`
	Permissions    []PermissionRule `json:"permissions,omitempty"`
}

// Execution attributes shared by every agent definition. These are the single
// source of truth for how a definition runs; there is no separate worker-kind
// tag. Classification is derived:
//
//	session  = OutputStream + SessionBindingShared
//	subagent = OutputCaller
//	utility  = StrategyOneshot
const (
	// Strategy: how many completions/tool iterations the definition runs.
	StrategyReact   = "react"   // ReAct tool loop (bounded by MaxRounds)
	StrategyOneshot = "oneshot" // a single completion; no tools

	// Context: how the LLM message list is assembled.
	ContextWindowed     = "windowed"     // bound session, truncated to token budget
	ContextConversation = "conversation" // full bound session (compaction floor ignored)
	ContextIncremental  = "incremental"  // messages after the compaction floor
	ContextRecent       = "recent"       // last Settings["recent_messages"] messages
	ContextTask         = "task"         // fresh [system, input]
	ContextNone         = "none"         // [system, input], no history

	// Output: the result sink. Transport is always streaming.
	OutputStream      = "stream"       // post deltas to the bus + completion
	OutputCaller      = "caller"       // return content to the invoking caller
	OutputSummary     = "summary"      // caller stores as the session summary
	OutputSessionMeta = "session_meta" // caller stores under Settings["meta_key"]
	OutputNone        = "none"         // discard

	// SessionBinding: session identity + persistence.
	SessionBindingShared   = "shared"   // one persistent channel:chatID session
	SessionBindingScratch  = "scratch"  // ephemeral namespaced "<session>:<role>"
	SessionBindingIsolated = "isolated" // no session read/write
)

// SettingRecentMessages bounds ContextRecent (a message count).
const SettingRecentMessages = "recent_messages"

// AgentTypeDef is the platform-owned policy blueprint for agents. Execution is
// driven entirely by Strategy/Context/Output/SessionBinding plus MaxRounds.
//
// Settings carries runtime policy/tool knobs that used to live as flat
// service_config_items under the "agent" component. Keys (all string-encoded;
// absent/empty = inherit the runtime code default):
//
//	max_tool_result_chars, summarize_token_percent,
//	summarize_message_threshold, memory_recent_days,
//	session_mode (continuous|split|manual), session_idle_timeout (duration),
//	session_max_messages, restrict_to_workspace (true|false),
//	shell_timeout (duration), recent_messages (count for ContextRecent)
//
// Loop limit is MaxRounds; the base prompt is Prompt. Role is a short
// invocation tag (e.g. "general", "title") used to key helper definitions.
type AgentTypeDef struct {
	ID          string `json:"id"`
	Role        string `json:"role,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Prompt      string `json:"prompt,omitempty"`
	MaxRounds   int    `json:"max_rounds,omitempty"`
	// Strategy is the execution shape: react (tool loop) | oneshot.
	Strategy string `json:"strategy,omitempty"`
	// Context is the input assembly strategy: windowed | conversation |
	// incremental | recent | task | none.
	Context string `json:"context,omitempty"`
	// Output is the result sink: stream | caller | summary | session_meta | none.
	Output string `json:"output,omitempty"`
	// SessionBinding is shared | scratch | isolated.
	SessionBinding   string                  `json:"session_binding,omitempty"`
	ChannelFallbacks map[string]string       `json:"channel_fallbacks,omitempty"`
	Permissions      []PermissionRule        `json:"permissions,omitempty"`
	Settings         map[string]string       `json:"settings,omitempty"`
	Seeded           bool                    `json:"seeded,omitempty"`
	Models           []TypeModelBinding      `json:"models,omitempty"`
	Capabilities     []TypeCapabilityBinding `json:"capabilities,omitempty"`
	// Override is the user's per-(agent, type) override of the baseline above.
	// It is populated only in the agent snapshot's Agents list (User plane);
	// admin/user catalog endpoints leave it nil. Absent fields inherit.
	Override *AgentTypeOverride `json:"override,omitempty"`
}

// AgentTypeOverride is a user's per-(agent, type) override of an AgentType's
// baseline configuration. It is the user layer of the live resolution
// (platform baseline -> override, last-match-wins; admin locks still win).
type AgentTypeOverride struct {
	ModelID      int64                  `json:"model_id,omitempty"`
	Thinking     string                 `json:"thinking,omitempty"`
	Settings     map[string]string      `json:"settings,omitempty"`
	Permissions  []PermissionRule       `json:"permissions,omitempty"`
	Capabilities []CapabilityAttachment `json:"capabilities,omitempty"`
}

// AgentTypeSaveRequest is the admin write shape for an AgentType.
type AgentTypeSaveRequest struct {
	Type AgentTypeDef `json:"type"`
}

// ListAgentTypesRequest lists agent definitions.
type ListAgentTypesRequest struct {
	// SessionOnly restricts the list to session-capable definitions
	// (output=stream, session_binding=shared); used by the user catalog.
	SessionOnly bool `json:"session_only,omitempty"`
}

// AgentTypesResponse carries a list of AgentType definitions.
type AgentTypesResponse struct {
	Types []AgentTypeDef `json:"types"`
}

// PermissionKeysResponse carries the admin-authored permission key catalog.
type PermissionKeysResponse struct {
	Keys []PermissionKeyDef `json:"keys"`
}

// SkillSaveRequest persists a user-scoped skill (agent authoring or portal).
type SkillSaveRequest struct {
	ID          string            `json:"id,omitempty"`
	UserID      string            `json:"user_id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Files       map[string]string `json:"files"`
	Origin      string            `json:"origin,omitempty"` // human | agent
	CreatedBy   string            `json:"created_by,omitempty"`
}

// SkillDeleteRequest removes a user-scoped skill.
type SkillDeleteRequest struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	UserID string `json:"user_id"`
}

// SkillAckRequest records a user's acknowledgement of a platform skill version.
type SkillAckRequest struct {
	UserID  string `json:"user_id"`
	SkillID string `json:"skill_id"`
	Version int    `json:"version"`
}

// SetAgentPermissionRequest replaces an agent's instance permission overrides
// (the user-layer rules appended after the AgentType baseline). Used to persist
// an "always allow" answer.
type SetAgentPermissionRequest struct {
	AgentID string `json:"agent_id"`
	// TypeID scopes the override to one mode. Empty targets the agent's primary
	// type (or, when Rule is set, the active mode resolved by the caller).
	TypeID      string           `json:"type_id,omitempty"`
	Permissions []PermissionRule `json:"permissions"`
}

// ======== Model Catalog Types ========

// ModelProviderDef is a provider connection (base URL + credentials). Providers
// are scoped like other catalog items: global (platform, admin-managed) or user
// (owner-managed, user supplies their own credentials).
type ModelProviderDef struct {
	ID          int64  `json:"id"`
	Scope       string `json:"scope"` // global | user
	OwnerUserID string `json:"owner_user_id,omitempty"`
	Name        string `json:"name"`
	Kind        string `json:"kind"` // openai | anthropic | google (openai = OpenAI-compatible)
	BaseURL     string `json:"base_url"`
	APIKey      string `json:"api_key,omitempty"` // masked in portal listings
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

// ModelDef is one model under a provider with its per-model parameters.
type ModelDef struct {
	ID              int64  `json:"id"`
	ProviderID      int64  `json:"provider_id"`
	Scope           string `json:"scope"` // global | user
	OwnerUserID     string `json:"owner_user_id,omitempty"`
	Name            string `json:"name"`  // display name
	Model           string `json:"model"` // provider API model id
	ContextWindow   int    `json:"context_window"`
	MaxOutputTokens int    `json:"max_output_tokens"`
	ThinkingEnabled bool   `json:"thinking_enabled"`
	ThinkingEffort  string `json:"thinking_effort,omitempty"` // low | medium | high
	Params          string `json:"params,omitempty"`          // JSON: temperature, top_p, ...
	// Provider fields are populated on agent-facing resolution so the runtime
	// has everything needed without a second lookup.
	ProviderKind    string `json:"provider_kind,omitempty"`
	ProviderBaseURL string `json:"provider_base_url,omitempty"`
	ProviderAPIKey  string `json:"provider_api_key,omitempty"`
	CreatedAt       string `json:"created_at,omitempty"`
	UpdatedAt       string `json:"updated_at,omitempty"`
}

// GetAgentSnapshotRequest fetches everything the agent reconciler needs in one
// round trip: agent record + config items + resolved capabilities (+ models).
type GetAgentSnapshotRequest struct {
	AgentID string `json:"agent_id"`
}

// AgentSnapshotResponse is the consolidated reconciler input.
type AgentSnapshotResponse struct {
	Agent        AgentInfo            `json:"agent"`
	Config       []ConfigItem         `json:"config"`
	Capabilities CapabilitiesResponse `json:"capabilities"`
	// Agents is the unified registry view for this agent: its session
	// agents plus every non-session definition (subagent/utility) the
	// runtime may invoke. Each carries Strategy/Context/Output.
	Agents []AgentTypeDef `json:"agents,omitempty"`
}

// BuiltinToolsSyncRequest is sent by an agent at startup to upsert its builtin
// tool definitions into the catalog (definitions only; admin state preserved).
type BuiltinToolsSyncRequest struct {
	AgentID string           `json:"agent_id"`
	Tools   []BuiltinToolDef `json:"tools"`
}

// ======== Streaming Types ========

// Outbound chunk kinds. The kind lets the gateway decide how to surface each
// streaming event per channel: web renders all kinds; batch channels surface
// only the final answer (content).
const (
	ChunkKindContent    = "content"     // answer text (default)
	ChunkKindThought    = "thought"     // model reasoning, streamed live
	ChunkKindToolUse    = "tool_use"    // calling a tool; Content = JSON {"name","arguments"}
	ChunkKindToolResult = "tool_result" // tool output; Content = (truncated) result text
	ChunkKindStatus     = "status"      // lifecycle/status event; Content = JSON {"event","state"}
	ChunkKindQuestion   = "question"    // interactive question awaiting a user answer; Content = JSON {id, questions}
	ChunkKindTodo       = "todo"        // updated todo list; Content = JSON [{content, status, priority}]
	ChunkKindApproval   = "approval"    // permission approval; Content = JSON PermissionApproval
	ChunkKindSummary    = "summary"     // compacted-context summary streaming under the compaction divider
)

// Turn-failure categories carried on OutboundComplete.Error. Gateways and
// clients map these to user-facing, per-channel wording; ErrorDetail carries a
// sanitized, human-readable supplement (never secrets or raw URLs). An empty
// Error means the turn completed successfully.
const (
	TurnErrorRateLimited         = "rate_limited"         // provider returned 429
	TurnErrorProviderUnavailable = "provider_unavailable" // 5xx, network, hung stream
	TurnErrorTimeout             = "request_timeout"      // agent-side TTFB/idle/request deadline (client timeout, not upstream outage)
	TurnErrorProviderAuth        = "provider_auth"        // provider rejected credentials (401/403)
	TurnErrorProviderNotFound    = "provider_not_found"   // model/endpoint not found (404)
	TurnErrorContextTooLong      = "context_too_long"     // context exceeded even after compression
	TurnErrorToolCallIncomplete  = "tool_call_incomplete" // the model's tool-call arguments were not valid JSON; the call was reported back and the model did not repair it
	TurnErrorMaxIterations       = "max_iterations"       // tool loop exhausted without a final answer
	TurnErrorStopped             = "stopped"              // user stopped the turn mid-flight
	TurnErrorInternal            = "internal"             // anything else
)

type OutboundChunk struct {
	MessageID string `json:"message_id"`
	Seq       int    `json:"seq"`
	Kind      string `json:"kind,omitempty"` // content | thought | tool_use | tool_result
	Content   string `json:"content"`
	AgentID   string `json:"agent_id,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Channel   string `json:"channel,omitempty"`
	ChatID    string `json:"chat_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
}

type OutboundComplete struct {
	MessageID   string `json:"message_id"`
	FullContent string `json:"full_content"`
	Summary     string `json:"summary,omitempty"`
	// Error is a TurnError* category when the turn failed; empty on success.
	Error       string `json:"error,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	UserID      string `json:"user_id,omitempty"`
	Channel     string `json:"channel,omitempty"`
	ChatID      string `json:"chat_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
}

// ======== Hypervisor Tunnel Types ========

type TunnelRegister struct {
	AgentID string `json:"agent_id"`
	UserID  string `json:"user_id"`
}

type TunnelRegistered struct {
	AgentID string `json:"agent_id"`
}

type TunnelRequest struct {
	RequestID string              `json:"request_id"`
	Method    string              `json:"method"`
	Path      string              `json:"path"`
	Headers   map[string][]string `json:"headers"`
	Body      []byte              `json:"body,omitempty"`
}

type TunnelResponse struct {
	RequestID string              `json:"request_id"`
	Status    int                 `json:"status"`
	Headers   map[string][]string `json:"headers"`
	Body      []byte              `json:"body"`
}

// TunnelStreamOpen asks the hypervisor to spawn (or attach to) a terminal
// session. StreamID is chosen by the bus so it is stable across reconnects.
type TunnelStreamOpen struct {
	StreamID string `json:"stream_id"`
	AgentID  string `json:"agent_id"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
}

// TunnelStreamInput carries PTY keystrokes from the browser to the hypervisor.
type TunnelStreamInput struct {
	StreamID string `json:"stream_id"`
	Data     []byte `json:"data"`
}

// TunnelStreamResize updates the PTY window size.
type TunnelStreamResize struct {
	StreamID string `json:"stream_id"`
	Cols     int    `json:"cols"`
	Rows     int    `json:"rows"`
}

// TunnelStreamClose terminates a terminal session.
type TunnelStreamClose struct {
	StreamID string `json:"stream_id"`
}

// TunnelStreamOutput carries PTY output from the hypervisor back to the bus.
type TunnelStreamOutput struct {
	StreamID string `json:"stream_id"`
	Data     []byte `json:"data"`
}

// TunnelStreamClosed is the final frame for a terminal session (bash exited,
// killed, or reaped).
type TunnelStreamClosed struct {
	StreamID string `json:"stream_id"`
	Reason   string `json:"reason,omitempty"`
}

// ======== Agent Routing Types ========

type AgentRecord struct {
	ID                    string `json:"id"`
	UserID                string `json:"user_id"`
	TypeID                string `json:"type_id,omitempty"`
	DisplayName           string `json:"display_name"`
	SystemPrompt          string `json:"system_prompt"`
	ModelOverride         string `json:"model_override"`
	ModelID               int64  `json:"model_id,omitempty"`
	Thinking              string `json:"thinking,omitempty"`
	McpEnabled            *bool  `json:"mcp_enabled,omitempty"`
	TitleModel            string `json:"title_model,omitempty"`
	SummarizeModel        string `json:"summarize_model,omitempty"`
	SubagentModel         string `json:"subagent_model,omitempty"`
	DefaultAgent          string `json:"default_agent,omitempty"`
	MaxIterations         int    `json:"max_iterations"`
	MaxConcurrentMessages int    `json:"max_concurrent_messages"`
	Status                string `json:"status"`
}

type ConfigChange struct {
	AgentID string   `json:"agent_id"`
	Keys    []string `json:"keys"`
}

// ======== Binding Agent Preferences ========

// GetBindingAgentRequest asks the bus which agent a channel binding talks to by
// default (explicit binding_agent_preferences entry, falling back to the user's
// first active agent).
type GetBindingAgentRequest struct {
	BindingID int64 `json:"binding_id"`
}

// GetBindingAgentResponse is the resolved default agent for a binding.
type GetBindingAgentResponse struct {
	AgentID string `json:"agent_id"`
}

// SetBindingAgentRequest assigns the default agent for a channel binding.
type SetBindingAgentRequest struct {
	BindingID int64  `json:"binding_id"`
	AgentID   string `json:"agent_id"`
}

// SetBindingAgentResponse confirms the assignment.
type SetBindingAgentResponse struct {
	BindingID int64  `json:"binding_id"`
	AgentID   string `json:"agent_id"`
}

// ======== Session Types ========

type SessionMeta struct {
	ID        string `json:"id"`
	AgentID   string `json:"agent_id"`
	Channel   string `json:"channel"`
	ChatID    string `json:"chat_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type ListSessionsRequest struct {
	AgentID string `json:"agent_id"`
	Limit   int    `json:"limit,omitempty"`
	Offset  int    `json:"offset,omitempty"`
}

type ListSessionsResponse struct {
	Sessions []SessionMeta `json:"sessions"`
}

type GetSessionRequest struct {
	SessionID string `json:"session_id"`
}

type GetSessionResponse struct {
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id"`
	Messages  string `json:"messages"`
	Summary   string `json:"summary"`
}

type SaveSessionRequest struct {
	SessionID   string `json:"session_id"`
	AgentID     string `json:"agent_id"`
	AccountID   string `json:"account_id,omitempty"`
	Channel     string `json:"channel"`
	ChatID      string `json:"chat_id"`
	Messages    string `json:"messages"` // JSON array of provider messages
	Summary     string `json:"summary,omitempty"`
	Title       string `json:"title,omitempty"` // optional: LLM-generated title (overrides heuristic)
	Meta        string `json:"meta,omitempty"`
	Status      string `json:"status,omitempty"`       // "active" | "archived"
	TurnRunning bool   `json:"turn_running,omitempty"` // a turn is being processed for this session

	// MessagesOmitted says Messages carries only the messages from FromIndex on
	// rather than the whole conversation. Its zero value means "the whole array",
	// which is what every older agent sends, so a new bus serves an old agent
	// exactly as before.
	//
	// ⚠️ The other order is not symmetric: a new agent talking to an *old* bus
	// would have its suffix taken for the whole conversation and would lose the
	// messages before FromIndex. Deploy the bus first, then the agents.
	MessagesOmitted bool `json:"messages_omitted,omitempty"`
	// FromIndex is the position in the conversation that Messages starts at. A
	// session's messages are only ever appended to, so "the messages I have not
	// sent yet" is a suffix — and the receiver checks the index it holds against
	// this one before appending, because a snapshot that failed to reach it would
	// otherwise leave a hole.
	FromIndex int `json:"from_index,omitempty"`
}

// ErrSessionResync is returned by Client.SaveSession when the bus asked for a
// full snapshot instead of applying a delta.
var ErrSessionResync = errors.New("bus asked for a full session snapshot")

// SaveSessionResponse answers a save. Resync says the receiver could not apply a
// delta — the index it holds does not match FromIndex — so the sender must send
// the whole array next time.
type SaveSessionResponse struct {
	SessionID string `json:"session_id"`
	Resync    bool   `json:"resync,omitempty"`
}

// ======== Audit Types ========

type GetAuditLogRequest struct {
	TargetType string `json:"target_type,omitempty"`
	TargetID   string `json:"target_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type AuditEntry struct {
	ID         int64  `json:"id"`
	ActorID    string `json:"actor_id"`
	ActorRole  string `json:"actor_role"`
	Action     string `json:"action"`
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	Details    string `json:"details"`
	CreatedAt  string `json:"created_at"`
}

type GetAuditLogResponse struct {
	Entries []AuditEntry `json:"entries"`
}
