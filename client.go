package nanoprotocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// levelVerbose is a custom slog level below DEBUG for high-frequency protocol messages.
const levelVerbose = slog.Level(-8)

var ErrConnectionClosed = errors.New("connection closed")

// SSEEvent is a single parsed server-sent event.
type SSEEvent struct {
	Event string
	ID    string
	Data  string
}

// Option configures a Client.
type Option func(*Client)

// WithToken sets a JWT sent as an Authorization header on every request.
func WithToken(token string) Option {
	return func(c *Client) {
		c.token = token
	}
}

// WithHTTPClient sets a custom HTTP client (used for testing and timeouts).
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.http = httpClient
	}
}

// Client is an HTTP client for communicating with the bus service.
// The bus exposes the nanoprotocol message envelope over POST /api/v1/rpc.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// NewClient creates a client connected to the given bus URL.
// The URL may be ws://, wss://, http:// or https:// — the WebSocket schemes
// are normalized to HTTP.
func NewClient(url string, opts ...Option) (*Client, error) {
	baseURL := normalizeBaseURL(url)
	if baseURL == "" {
		return nil, fmt.Errorf("invalid bus url %q", url)
	}

	c := &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 60 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 60 * time.Second}
	}
	return c, nil
}

// normalizeBaseURL converts a ws:// or wss:// URL into its HTTP equivalent.
func normalizeBaseURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	case "http", "https":
	default:
		return ""
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String()
}

// Close releases the client's idle connections. Provided for interface parity
// with the legacy WebSocket client.
func (c *Client) Close() error {
	if c.http != nil {
		c.http.CloseIdleConnections()
	}
	return nil
}

// Send sends a message to the bus and waits for a response.
// A nil response is returned when the bus replies 204 (no payload).
func (c *Client) Send(msg Message) (*Message, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.baseURL+"/api/v1/rpc", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	slog.Log(context.Background(), levelVerbose, "protocol: sending message", "type", msg.Type)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send %s: %w", msg.Type, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bus rpc failed (%d): %s", resp.StatusCode, respBody)
	}

	var out Message
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	slog.Log(context.Background(), levelVerbose, "protocol: received response", "type", out.Type)
	return &out, nil
}

// ResolveUser asks the bus to resolve a platform identity to a user ID.
func (c *Client) ResolveUser(channel, senderID string) (*ResolveUserResponse, error) {
	msg, err := NewMessage(TypeResolveUser, ResolveUserRequest{
		Channel:  channel,
		SenderID: senderID,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("resolve user: %s", resp.Error)
	}

	var result ResolveUserResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// PostInbound posts an inbound message to the bus.
func (c *Client) PostInbound(inbound InboundMessage) error {
	msg, err := NewMessage(TypePostInbound, inbound)
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("post inbound: %s", resp.Error)
	}
	return nil
}

// PollInbound polls for a pending inbound message for the given user or binding.
func (c *Client) PollInbound(userID string, bindingID int64, agentID string) (*InboundMessage, error) {
	msg, err := NewMessage(TypePollInbound, PollInboundRequest{
		UserID:    userID,
		BindingID: bindingID,
		AgentID:   agentID,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		slog.Debug("poll inbound failed", "error", err)
		return nil, nil
	}
	if resp == nil {
		return nil, nil
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("poll inbound: %s", resp.Error)
	}
	if resp.Payload == nil {
		return nil, nil
	}

	var inbound InboundMessage
	if err := resp.DecodePayload(&inbound); err != nil {
		return nil, err
	}
	return &inbound, nil
}

// PostOutbound posts an outbound message to the bus.
func (c *Client) PostOutbound(outbound OutboundMessage) error {
	msg, err := NewMessage(TypePostOutbound, outbound)
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("post outbound: %s", resp.Error)
	}
	return nil
}

// PostOutboundChunk posts a streaming chunk to the bus's outbound SSE stream.
func (c *Client) PostOutboundChunk(chunk OutboundChunk) error {
	if chunk.Channel == "" {
		return fmt.Errorf("post outbound chunk: channel is required")
	}
	if err := c.postJSON("/api/v1/outbound/chunk", chunk); err != nil {
		return fmt.Errorf("post outbound chunk: %w", err)
	}
	return nil
}

// PostOutboundComplete posts the final assembled message for a streaming turn.
func (c *Client) PostOutboundComplete(complete OutboundComplete) error {
	if complete.Channel == "" {
		return fmt.Errorf("post outbound complete: channel is required")
	}
	if err := c.postJSON("/api/v1/outbound/complete", complete); err != nil {
		return fmt.Errorf("post outbound complete: %w", err)
	}
	return nil
}

// postJSON performs a JSON POST to the given path (used for the streaming
// outbound endpoints) and returns an error for non-2xx responses.
func (c *Client) postJSON(path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bus %s failed (%d): %s", path, resp.StatusCode, respBody)
	}
	return nil
}

// PollOutbound polls for a pending outbound message for the given channel.
func (c *Client) PollOutbound(channel, gatewayID string) (*OutboundMessage, error) {
	msg, err := NewMessage(TypePollOutbound, PollOutboundRequest{
		Channel:   channel,
		GatewayID: gatewayID,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		slog.Debug("poll outbound failed", "error", err)
		return nil, nil
	}
	if resp == nil {
		return nil, nil
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("poll outbound: %s", resp.Error)
	}
	if resp.Payload == nil {
		return nil, nil
	}

	var outbound OutboundMessage
	if err := resp.DecodePayload(&outbound); err != nil {
		return nil, err
	}
	return &outbound, nil
}

// PollPendingContainers polls the bus for pending container provisioning requests.
func (c *Client) PollPendingContainers(runnerID string) ([]ContainerRequest, error) {
	msg, err := NewMessage(TypePollPendingContainers, PollPendingContainersRequest{
		RunnerID: runnerID,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("poll pending containers: %s", resp.Error)
	}
	if resp.Payload == nil {
		return nil, nil
	}

	var result PollPendingContainersResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return result.Requests, nil
}

// RequestContainer asks the bus to enqueue a container provisioning request.
func (c *Client) RequestContainer(userID string, bindingID int64, agentID, level string) error {
	msg, err := NewMessage(TypeRequestContainer, RequestContainerRequest{
		UserID:    userID,
		BindingID: bindingID,
		AgentID:   agentID,
		Level:     level,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	if resp.Type == TypeError {
		return fmt.Errorf("request container: %s", resp.Error)
	}
	return nil
}

// ListContainers asks the bus for a user's container requests.
func (c *Client) ListContainers(userID string, limit int) ([]ContainerRequest, error) {
	msg, err := NewMessage(TypeListContainers, ListContainersRequest{UserID: userID, Limit: limit})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("list containers: %s", resp.Error)
	}

	var result ListContainersResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return result.Requests, nil
}

// UpdateContainerStatus updates the status of a container in the bus.
func (c *Client) UpdateContainerStatus(userID string, bindingID int64, status string) error {
	msg, err := NewMessage(TypeUpdateContainerStatus, UpdateContainerStatusRequest{
		UserID:    userID,
		BindingID: bindingID,
		Status:    status,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("update container status: %s", resp.Error)
	}
	return nil
}

// DisposeContainer asks the bus to enqueue a container disposal request.
func (c *Client) DisposeContainer(userID string, bindingID int64, agentID string) error {
	msg, err := NewMessage(TypeDisposeContainer, DisposeContainerRequest{
		UserID:    userID,
		BindingID: bindingID,
		AgentID:   agentID,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	if resp.Type == TypeError {
		return fmt.Errorf("dispose container: %s", resp.Error)
	}
	return nil
}

// UpdateUserStatus updates the status of a user in the bus.
func (c *Client) UpdateUserStatus(userID, status string) error {
	msg, err := NewMessage(TypeUpdateUserStatus, UpdateUserStatusRequest{
		UserID: userID,
		Status: status,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("update user status: %s", resp.Error)
	}
	return nil
}

// GetUserInfo retrieves user information from the bus.
func (c *Client) GetUserInfo(userID string) (*UserInfoResponse, error) {
	msg, err := NewMessage(TypeGetUserInfo, GetUserInfoRequest{
		UserID: userID,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get user info: %s", resp.Error)
	}

	var result UserInfoResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetWeChatKFSyncState retrieves the current WeChat KF sync state for the given account.
func (c *Client) GetWeChatKFSyncState(openKfID string) (*WeChatKFSyncStateResponse, error) {
	msg, err := NewMessage(TypeGetWeChatKFSyncState, GetWeChatKFSyncStateRequest{
		OpenKfID: openKfID,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get wechatkf sync state: %s", resp.Error)
	}

	var result WeChatKFSyncStateResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	if !result.Found {
		return nil, nil
	}
	return &result, nil
}

// UpdateWeChatKFCursor persists the latest processed sync cursor for a WeChat KF account.
func (c *Client) UpdateWeChatKFCursor(openKfID, cursor string) error {
	msg, err := NewMessage(TypeUpdateWeChatKFCursor, UpdateWeChatKFCursorRequest{
		OpenKfID: openKfID,
		Cursor:   cursor,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("update wechatkf cursor: %s", resp.Error)
	}
	return nil
}

// UpdateWeChatKFCallbackToken persists the latest callback token for a WeChat KF account.
func (c *Client) UpdateWeChatKFCallbackToken(openKfID, callbackToken string, tokenExpiresAt time.Time) error {
	msg, err := NewMessage(TypeUpdateWeChatKFCallbackTok, UpdateWeChatKFCallbackTokenRequest{
		OpenKfID:       openKfID,
		CallbackToken:  callbackToken,
		TokenExpiresAt: tokenExpiresAt,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("update wechatkf callback token: %s", resp.Error)
	}
	return nil
}

// GetConfigItems retrieves all configuration items for the given component type from the bus.
func (c *Client) GetConfigItems(componentType string) ([]ConfigItem, error) {
	msg, err := NewMessage(TypeGetConfigItems, GetConfigItemsRequest{
		ComponentType: componentType,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get config items: %s", resp.Error)
	}

	var result GetConfigItemsResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// GetCapabilities fetches the capability catalog (builtin tools, MCP servers,
// skills) scoped to the given agent, including its attachments. The agent
// receives unmasked credentials (it must execute the servers).
func (c *Client) GetCapabilities(agentID string) (*CapabilitiesResponse, error) {
	msg, err := NewMessage(TypeGetCapabilities, GetCapabilitiesRequest{AgentID: agentID})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get capabilities: %s", resp.Error)
	}
	var result CapabilitiesResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAgentSnapshot fetches the full reconciler input for an agent in one round
// trip: agent record + agent config items + resolved capabilities (including
// the visible model catalog). Preferred over separate GetCapabilities+config
// calls on every change.
func (c *Client) GetAgentSnapshot(agentID string) (*AgentSnapshotResponse, error) {
	msg, err := NewMessage(TypeGetAgentSnapshot, GetAgentSnapshotRequest{AgentID: agentID})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get agent snapshot: %s", resp.Error)
	}
	var result AgentSnapshotResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SyncBuiltinTools upserts the agent binary's builtin tool definitions into the
// bus catalog (definitions only — admin state like default_enabled/locked is
// preserved by the bus).
func (c *Client) SyncBuiltinTools(agentID string, tools []BuiltinToolDef) error {
	msg, err := NewMessage(TypeBuiltinToolsSync, BuiltinToolsSyncRequest{
		AgentID: agentID,
		Tools:   tools,
	})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return fmt.Errorf("sync builtin tools: %s", resp.Error)
	}
	return nil
}

// ListAgentTypes fetches the platform agent definitions. When sessionOnly is
// true only session-capable definitions (output=stream, binding=shared) are
// returned.
func (c *Client) ListAgentTypes(sessionOnly bool) ([]AgentTypeDef, error) {
	msg, err := NewMessage(TypeListAgentTypes, ListAgentTypesRequest{SessionOnly: sessionOnly})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("list agent types: %s", resp.Error)
	}
	var result AgentTypesResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return result.Types, nil
}

// SetAgentPermission persists a per-agent permission override (e.g. "always
// allow") on the agent's capability binding.
func (c *Client) SetAgentPermission(req SetAgentPermissionRequest) error {
	msg, err := NewMessage(TypeSetAgentPermission, req)
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return fmt.Errorf("set agent permission: %s", resp.Error)
	}
	return nil
}

// SaveSkill creates or updates a user-scoped skill on the bus. Used by the
// agent's manage_skill tool; the bus enforces ownership and name uniqueness.
func (c *Client) SaveSkill(req SkillSaveRequest) (*SkillDef, error) {
	msg, err := NewMessage(TypeSkillSave, req)
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("save skill: %s", resp.Error)
	}
	var def SkillDef
	if err := resp.DecodePayload(&def); err != nil {
		return nil, err
	}
	return &def, nil
}

// DeleteSkill removes a user-scoped skill owned by userID. Either id or name
// may be supplied (name is resolved owner-scoped by the bus).
func (c *Client) DeleteSkill(id, name, userID string) error {
	msg, err := NewMessage(TypeSkillDelete, SkillDeleteRequest{ID: id, Name: name, UserID: userID})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return fmt.Errorf("delete skill: %s", resp.Error)
	}
	return nil
}

// AckSkill records a user's acknowledgement of a platform skill version.
func (c *Client) AckSkill(userID, skillID string, version int) error {
	msg, err := NewMessage(TypeSkillAck, SkillAckRequest{UserID: userID, SkillID: skillID, Version: version})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return fmt.Errorf("ack skill: %s", resp.Error)
	}
	return nil
}

// SetConfigItem updates a single configuration item on the bus.
func (c *Client) SetConfigItem(componentType, key, value string) error {
	msg, err := NewMessage(TypeSetConfigItem, SetConfigItemRequest{
		ComponentType: componentType,
		Key:           key,
		Value:         value,
	})
	if err != nil {
		return err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return fmt.Errorf("set config item: %s", resp.Error)
	}
	return nil
}

// RegisterRunner registers this runner instance with the bus.
func (c *Client) RegisterRunner(runnerID, hostname, dockerHost string) (*RegisterRunnerResponse, error) {
	msg, err := NewMessage(TypeRegisterRunner, RegisterRunnerRequest{
		RunnerID:   runnerID,
		Hostname:   hostname,
		DockerHost: dockerHost,
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("register runner: %s", resp.Error)
	}

	var result RegisterRunnerResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ======== Auth ========

// Authenticate exchanges an email and password for a JWT (delegated to nanoauth by the bus).
func (c *Client) Authenticate(email, password string) (*AuthenticateResponse, error) {
	msg, err := NewMessage(TypeAuthenticate, AuthenticateRequest{Email: email, Password: password})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("authenticate: %s", resp.Error)
	}
	var result AuthenticateResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ValidateToken asks the bus to validate a JWT (delegated to nanoauth).
func (c *Client) ValidateToken(token string) (*ValidateTokenResponse, error) {
	msg, err := NewMessage(TypeValidateToken, ValidateTokenRequest{Token: token})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("validate token: %s", resp.Error)
	}
	var result ValidateTokenResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RegisterAccount creates a new account (delegated to nanoauth by the bus).
func (c *Client) RegisterAccount(email, password, displayName string) (*RegisterAccountResponse, error) {
	msg, err := NewMessage(TypeRegisterAccount, RegisterAccountRequest{
		Email:       email,
		Password:    password,
		DisplayName: displayName,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("register account: %s", resp.Error)
	}
	var result RegisterAccountResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ======== Agent Management ========

// CreateAgent creates a new agent for a user.
func (c *Client) CreateAgent(req CreateAgentRequest) error {
	msg, err := NewMessage(TypeCreateAgent, req)
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("create agent: %s", resp.Error)
	}
	return nil
}

// UpdateAgent updates an existing agent.
func (c *Client) UpdateAgent(req UpdateAgentRequest) error {
	msg, err := NewMessage(TypeUpdateAgent, req)
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("update agent: %s", resp.Error)
	}
	return nil
}

// DeleteAgent soft-deletes an agent.
func (c *Client) DeleteAgent(agentID string) error {
	msg, err := NewMessage(TypeDeleteAgent, DeleteAgentRequest{ID: agentID})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("delete agent: %s", resp.Error)
	}
	return nil
}

// UpdateAgentStatus reports an agent's runtime status to the bus.
func (c *Client) UpdateAgentStatus(userID, agentID, status string) error {
	msg, err := NewMessage(TypeUpdateAgentStatus, UpdateAgentStatusRequest{
		UserID:  userID,
		AgentID: agentID,
		Status:  status,
	})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("update agent status: %s", resp.Error)
	}
	return nil
}

// ListAgents lists the agents belonging to a user.
func (c *Client) ListAgents(userID string) ([]AgentRecord, error) {
	msg, err := NewMessage(TypeListAgents, ListAgentsRequest{UserID: userID})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("list agents: %s", resp.Error)
	}

	var result ListAgentsResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	records := make([]AgentRecord, 0, len(result.Agents))
	for _, a := range result.Agents {
		records = append(records, AgentRecord{
			ID:             a.ID,
			UserID:         a.UserID,
			DisplayName:    a.DisplayName,
			SystemPrompt:   a.SystemPrompt,
			ModelOverride:  a.Model,
			ModelID:        a.ModelID,
			Thinking:       a.Thinking,
			McpEnabled:     a.McpEnabled,
			TitleModel:     a.TitleModel,
			SummarizeModel: a.SummarizeModel,
			SubagentModel:  a.SubagentModel,
			Status:         a.Status,
		})
	}
	return records, nil
}

// ======== Agent-Scoped Config ========

// GetAgentConfig retrieves the configuration items for a specific agent.
func (c *Client) GetAgentConfig(agentID string) ([]ConfigItem, error) {
	msg, err := NewMessage(TypeGetAgentConfig, GetAgentConfigRequest{AgentID: agentID})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get agent config: %s", resp.Error)
	}
	var result GetAgentConfigResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// SetAgentConfig updates a single configuration key for a specific agent.
func (c *Client) SetAgentConfig(agentID, key, value string) error {
	msg, err := NewMessage(TypeSetAgentConfig, SetAgentConfigRequest{
		AgentID: agentID,
		Key:     key,
		Value:   value,
	})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("set agent config: %s", resp.Error)
	}
	return nil
}

// ======== Session Persistence ========

// SaveSession persists a session snapshot to the bus (stored in MySQL).
func (c *Client) SaveSession(req SaveSessionRequest) error {
	msg, err := NewMessage(TypeSaveSession, req)
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("save session: %s", resp.Error)
	}
	// A resync is an answer, not a failure: the bus could not apply a delta
	// because it does not hold the prefix it follows, and it wants the whole
	// conversation. It travels as an error because that is what this signature
	// has — and it is distinguishable, so the caller reacts by sending everything
	// rather than recording a failure.
	if resp != nil {
		var out SaveSessionResponse
		if err := resp.DecodePayload(&out); err == nil && out.Resync {
			return ErrSessionResync
		}
	}
	return nil
}

// ArchiveSession marks a session as archived.
func (c *Client) ArchiveSession(sessionID string) error {
	msg, err := NewMessage(TypeArchiveSession, GetSessionRequest{SessionID: sessionID})
	if err != nil {
		return err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return err
	}
	if resp != nil && resp.Type == TypeError {
		return fmt.Errorf("archive session: %s", resp.Error)
	}
	return nil
}

// ListSessions lists session metadata for an agent.
func (c *Client) ListSessions(agentID string, limit, offset int) ([]SessionMeta, error) {
	msg, err := NewMessage(TypeListSessions, ListSessionsRequest{
		AgentID: agentID,
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("list sessions: %s", resp.Error)
	}
	var result ListSessionsResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return result.Sessions, nil
}

// GetSession retrieves a full session by ID.
func (c *Client) GetSession(sessionID string) (*GetSessionResponse, error) {
	msg, err := NewMessage(TypeGetSession, GetSessionRequest{SessionID: sessionID})
	if err != nil {
		return nil, err
	}
	resp, err := c.Send(msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("no response")
	}
	if resp.Type == TypeError {
		return nil, fmt.Errorf("get session: %s", resp.Error)
	}
	var result GetSessionResponse
	if err := resp.DecodePayload(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ConfigChanges returns a channel of configuration change notifications.
// It subscribes to the bus SSE events stream and reconnects automatically.
func (c *Client) ConfigChanges() <-chan ConfigChange {
	ch := make(chan ConfigChange, 16)
	go c.streamConfigChanges(ch)
	return ch
}

// streamConfigChanges reads config_changed SSE events and forwards them.
func (c *Client) streamConfigChanges(out chan<- ConfigChange) {
	ctx := context.Background()
	lastID := ""
	for {
		events, err := c.subscribeSSE(ctx, "/api/v1/events/stream?component=agent", lastID)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ev := range events {
			if ev.ID != "" {
				lastID = ev.ID
			}
			if ev.Event != TypeConfigChanged {
				continue
			}
			var env struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal([]byte(ev.Data), &env); err != nil {
				slog.Debug("config change envelope decode error", "error", err)
				continue
			}
			var change ConfigChange
			if err := json.Unmarshal([]byte(env.Content), &change); err != nil {
				slog.Debug("config change decode error", "error", err)
				continue
			}
			select {
			case out <- change:
			default:
				// drop when the consumer is slow
			}
		}
		time.Sleep(time.Second)
	}
}

// AgentChanges returns a channel of agent change notifications (create, update,
// delete). It subscribes to the bus SSE events stream and reconnects
// automatically.
func (c *Client) AgentChanges() <-chan AgentChangeNotification {
	ch := make(chan AgentChangeNotification, 16)
	go c.streamAgentChanges(ch)
	return ch
}

// streamAgentChanges reads agents_changed SSE events and forwards them.
func (c *Client) streamAgentChanges(out chan<- AgentChangeNotification) {
	ctx := context.Background()
	lastID := ""
	for {
		events, err := c.subscribeSSE(ctx, "/api/v1/events/stream", lastID)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ev := range events {
			if ev.ID != "" {
				lastID = ev.ID
			}
			if ev.Event != TypeAgentsChanged {
				continue
			}
			var env struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal([]byte(ev.Data), &env); err != nil {
				slog.Debug("agent change envelope decode error", "error", err)
				continue
			}
			var change AgentChangeNotification
			if err := json.Unmarshal([]byte(env.Content), &change); err != nil {
				slog.Debug("agent change decode error", "error", err)
				continue
			}
			select {
			case out <- change:
			default:
				// drop when the consumer is slow
			}
		}
		time.Sleep(time.Second)
	}
}

// subscribeSSE opens a streaming SSE connection and returns a channel of events.
// The connection is terminated when the returned channel is closed.
func (c *Client) subscribeSSE(ctx context.Context, path, lastID string) (<-chan SSEEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("sse subscribe %s failed: %d", path, resp.StatusCode)
	}

	events := make(chan SSEEvent, 32)
	go func() {
		defer resp.Body.Close()
		defer close(events)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var ev SSEEvent
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "event:"):
				ev.Event = strings.TrimSpace(line[len("event:"):])
			case strings.HasPrefix(line, "id:"):
				ev.ID = strings.TrimSpace(line[len("id:"):])
			case strings.HasPrefix(line, "data:"):
				ev.Data += strings.TrimSpace(line[len("data:"):])
			case line == "":
				if ev.Event != "" || ev.Data != "" {
					events <- ev
				}
				ev = SSEEvent{}
			}
		}
	}()

	return events, nil
}
