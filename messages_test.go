package nanoprotocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewMessage(t *testing.T) {
	payload := ResolveUserRequest{Channel: "telegram", SenderID: "123"}
	msg, err := NewMessage(TypeResolveUser, payload)
	if err != nil {
		t.Fatalf("NewMessage() error = %v", err)
	}

	if msg.Type != TypeResolveUser {
		t.Errorf("Type = %q, want %q", msg.Type, TypeResolveUser)
	}
	if msg.Payload == nil {
		t.Fatal("Payload is nil")
	}
	if msg.Error != "" {
		t.Errorf("Error = %q, want empty", msg.Error)
	}
}

func TestNewErrorMessage(t *testing.T) {
	msg := NewErrorMessage("something went wrong")

	if msg.Type != TypeError {
		t.Errorf("Type = %q, want %q", msg.Type, TypeError)
	}
	if msg.Error != "something went wrong" {
		t.Errorf("Error = %q, want %q", msg.Error, "something went wrong")
	}
	if msg.Payload != nil {
		t.Errorf("Payload = %v, want nil", msg.Payload)
	}
}

func TestMessage_DecodePayload(t *testing.T) {
	original := ResolveUserRequest{Channel: "discord", SenderID: "456"}
	msg, err := NewMessage(TypeResolveUser, original)
	if err != nil {
		t.Fatalf("NewMessage() error = %v", err)
	}

	var decoded ResolveUserRequest
	if err := msg.DecodePayload(&decoded); err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}

	if decoded.Channel != original.Channel {
		t.Errorf("Channel = %q, want %q", decoded.Channel, original.Channel)
	}
	if decoded.SenderID != original.SenderID {
		t.Errorf("SenderID = %q, want %q", decoded.SenderID, original.SenderID)
	}
}

func TestMessage_DecodePayload_EmptyPayload(t *testing.T) {
	msg := Message{Type: TypeError}

	var target ResolveUserRequest
	if err := msg.DecodePayload(&target); err == nil {
		t.Error("DecodePayload() expected error for nil payload, got nil")
	}
}

func TestMessage_MarshalJSON(t *testing.T) {
	msg := NewErrorMessage("test error")
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.Type != TypeError {
		t.Errorf("Type = %q, want %q", decoded.Type, TypeError)
	}
	if decoded.Error != "test error" {
		t.Errorf("Error = %q, want %q", decoded.Error, "test error")
	}
}

func TestInboundMessage_MarshalRoundTrip(t *testing.T) {
	ts := time.Date(2026, 2, 10, 12, 0, 0, 0, time.UTC)
	original := InboundMessage{
		UserID:    "alice",
		SessionID: "telegram:123",
		Channel:   "telegram",
		ChatID:    "123",
		SenderID:  "sender1",
		MessageID: "msg-123",
		Content:   "Hello world",
		Media:     []MediaItem{{Platform: "wecom", MediaID: "photo123", Filename: "photo.jpg"}},
		Metadata:  map[string]any{"key": "value"},
		Timestamp: ts,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded InboundMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.UserID != original.UserID {
		t.Errorf("UserID = %q, want %q", decoded.UserID, original.UserID)
	}
	if decoded.SessionID != original.SessionID {
		t.Errorf("SessionID = %q, want %q", decoded.SessionID, original.SessionID)
	}
	if decoded.Content != original.Content {
		t.Errorf("Content = %q, want %q", decoded.Content, original.Content)
	}
	if decoded.MessageID != original.MessageID {
		t.Errorf("MessageID = %q, want %q", decoded.MessageID, original.MessageID)
	}
	if len(decoded.Media) != 1 || decoded.Media[0].Filename != "photo.jpg" {
		t.Errorf("Media = %v, want %v", decoded.Media, original.Media)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, original.Timestamp)
	}
}

func TestOutboundMessage_MarshalRoundTrip(t *testing.T) {
	original := OutboundMessage{
		UserID:    "bob",
		SessionID: "discord:789",
		Channel:   "discord",
		ChatID:    "789",
		MessageID: "out-42",
		Content:   "Hi there!",
		ReplyTo:   "msg-42",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded OutboundMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.UserID != original.UserID {
		t.Errorf("UserID = %q, want %q", decoded.UserID, original.UserID)
	}
	if decoded.Content != original.Content {
		t.Errorf("Content = %q, want %q", decoded.Content, original.Content)
	}
	if decoded.MessageID != original.MessageID {
		t.Errorf("MessageID = %q, want %q", decoded.MessageID, original.MessageID)
	}
	if decoded.ReplyTo != original.ReplyTo {
		t.Errorf("ReplyTo = %q, want %q", decoded.ReplyTo, original.ReplyTo)
	}
}

func TestOutboundMessage_OmitsEmptyReplyTo(t *testing.T) {
	msg := OutboundMessage{
		UserID:    "alice",
		SessionID: "cli:alice",
		Channel:   "cli",
		ChatID:    "alice",
		Content:   "response",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if _, ok := raw["reply_to"]; ok {
		t.Error("Expected reply_to to be omitted when empty")
	}
}

func TestResolveUserRequest_MarshalRoundTrip(t *testing.T) {
	original := ResolveUserRequest{Channel: "wecom", SenderID: "wx-user-1"}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded ResolveUserRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.Channel != original.Channel {
		t.Errorf("Channel = %q, want %q", decoded.Channel, original.Channel)
	}
	if decoded.SenderID != original.SenderID {
		t.Errorf("SenderID = %q, want %q", decoded.SenderID, original.SenderID)
	}
}

func TestPollInboundRequest_MarshalRoundTrip(t *testing.T) {
	original := PollInboundRequest{UserID: "alice", AgentID: "agent-uuid-1"}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded PollInboundRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.UserID != original.UserID {
		t.Errorf("UserID = %q, want %q", decoded.UserID, original.UserID)
	}
	if decoded.AgentID != original.AgentID {
		t.Errorf("AgentID = %q, want %q", decoded.AgentID, original.AgentID)
	}
}

func TestPollOutboundRequest_MarshalRoundTrip(t *testing.T) {
	original := PollOutboundRequest{Channel: "feishu", GatewayID: "gw-uuid-1"}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded PollOutboundRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.Channel != original.Channel {
		t.Errorf("Channel = %q, want %q", decoded.Channel, original.Channel)
	}
	if decoded.GatewayID != original.GatewayID {
		t.Errorf("GatewayID = %q, want %q", decoded.GatewayID, original.GatewayID)
	}
}

func TestWeChatKFSyncStateResponse_MarshalRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 22, 6, 0, 0, 0, time.UTC)
	original := WeChatKFSyncStateResponse{
		Found:          true,
		OpenKfID:       "kf_123",
		Cursor:         "cursor-1",
		CallbackToken:  "token-1",
		TokenExpiresAt: ts,
		UpdatedAt:      ts,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded WeChatKFSyncStateResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.OpenKfID != original.OpenKfID {
		t.Errorf("OpenKfID = %q, want %q", decoded.OpenKfID, original.OpenKfID)
	}
	if decoded.Cursor != original.Cursor {
		t.Errorf("Cursor = %q, want %q", decoded.Cursor, original.Cursor)
	}
	if decoded.CallbackToken != original.CallbackToken {
		t.Errorf("CallbackToken = %q, want %q", decoded.CallbackToken, original.CallbackToken)
	}
	if !decoded.TokenExpiresAt.Equal(original.TokenExpiresAt) {
		t.Errorf("TokenExpiresAt = %v, want %v", decoded.TokenExpiresAt, original.TokenExpiresAt)
	}
	if !decoded.UpdatedAt.Equal(original.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", decoded.UpdatedAt, original.UpdatedAt)
	}
}

func TestNewMessage_AsPayloadInEnvelope(t *testing.T) {
	// Test the full flow: create an inbound message, wrap in envelope, decode
	inbound := InboundMessage{
		UserID:    "carol",
		SessionID: "cli:carol",
		Channel:   "cli",
		ChatID:    "carol",
		SenderID:  "carol",
		MessageID: "cli-msg-1",
		Content:   "test message",
		Timestamp: time.Now().UTC().Truncate(time.Second),
	}

	envelope, err := NewMessage(TypeInbound, inbound)
	if err != nil {
		t.Fatalf("NewMessage() error = %v", err)
	}

	// Marshal and unmarshal the whole envelope (simulates WebSocket transfer)
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal(envelope) error = %v", err)
	}

	var received Message
	if err := json.Unmarshal(data, &received); err != nil {
		t.Fatalf("json.Unmarshal(envelope) error = %v", err)
	}

	if received.Type != TypeInbound {
		t.Errorf("Type = %q, want %q", received.Type, TypeInbound)
	}

	var decoded InboundMessage
	if err := received.DecodePayload(&decoded); err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}

	if decoded.Content != "test message" {
		t.Errorf("Content = %q, want %q", decoded.Content, "test message")
	}
	if decoded.UserID != "carol" {
		t.Errorf("UserID = %q, want %q", decoded.UserID, "carol")
	}
	if decoded.MessageID != "cli-msg-1" {
		t.Errorf("MessageID = %q, want %q", decoded.MessageID, "cli-msg-1")
	}
}

func TestTypeConstants(t *testing.T) {
	// Ensure type constants are distinct
	types := []string{
		TypeResolveUser,
		TypePostInbound,
		TypePollOutbound,
		TypeUserResolved,
		TypeOutbound,
		TypePollInbound,
		TypePostOutbound,
		TypeInbound,
		TypeGetWeChatKFSyncState,
		TypeWeChatKFSyncState,
		TypeUpdateWeChatKFCursor,
		TypeUpdateWeChatKFCallbackTok,
		TypeError,
	}

	seen := make(map[string]bool)
	for _, typ := range types {
		if typ == "" {
			t.Error("Found empty type constant")
		}
		if seen[typ] {
			t.Errorf("Duplicate type constant: %q", typ)
		}
		seen[typ] = true
	}
}

// TestAgentGroupField_MarshalRoundTrip pins the one shape still on this
// protocol: the listing's GroupID. The create/update request shapes moved to the
// portal with the agent rows (they are HTTP bodies there, not messages).
func TestAgentGroupField_MarshalRoundTrip(t *testing.T) {
	t.Run("agent info", func(t *testing.T) {
		body, err := json.Marshal(AgentInfo{ID: "a1", GroupID: "g1"})
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}
		var decoded AgentInfo
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		if decoded.GroupID != "g1" {
			t.Errorf("GroupID = %q, want %q", decoded.GroupID, "g1")
		}
	})
}
