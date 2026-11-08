package nanoprotocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockBusServer creates an HTTP server that mimics the bus /api/v1/rpc endpoint.
func mockBusServer(t *testing.T, handler func(*Message) *Message) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/rpc" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var msg Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		resp := handler(&msg)
		if resp == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(resp)
	}))
}

func TestNewClient(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message { return nil })
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()
}

func TestNewClient_NormalizesWSURL(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message { return nil })
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	client, err := NewClient(wsURL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()
}

func TestNewClient_BadURL(t *testing.T) {
	_, err := NewClient("://bad")
	if err == nil {
		t.Error("NewClient() expected error for invalid URL")
	}
}

func TestClient_Send(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		return &Message{Type: "response", Payload: msg.Payload}
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	msg := Message{Type: "test", Payload: []byte(`{"key":"value"}`)}
	resp, err := client.Send(msg)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if resp.Type != "response" {
		t.Errorf("response Type = %q, want %q", resp.Type, "response")
	}
}

func TestClient_Send_NoContent(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message { return nil })
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	resp, err := client.Send(Message{Type: "test"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if resp != nil {
		t.Errorf("Send() = %v, want nil", resp)
	}
}

func TestClient_ResolveUser(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		resp, _ := NewMessage(TypeUserResolved, ResolveUserResponse{UserID: "alice"})
		return &resp
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	result, err := client.ResolveUser("telegram", "123")
	if err != nil {
		t.Fatalf("ResolveUser() error = %v", err)
	}
	if result.UserID != "alice" {
		t.Errorf("userID = %q, want %q", result.UserID, "alice")
	}
}

func TestClient_ResolveUser_Error(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		resp := NewErrorMessage("user not found")
		return &resp
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	_, err = client.ResolveUser("telegram", "unknown")
	if err == nil {
		t.Error("ResolveUser() expected error")
	}
}

// Every request the client makes to the bus has to carry the shared token:
// the REST endpoints, the streaming posts and the SSE subscriptions. This is
// the contract the bus's authentication depends on.
func TestClient_TokenIsSentOnEveryPath(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get("Authorization")
		mu.Unlock()
		switch r.URL.Path {
		case "/api/v1/rpc":
			_ = json.NewEncoder(w).Encode(Message{Type: "ok"})
		case "/api/v1/outbound/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL, WithToken("s3cret"))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	if err := client.PostInbound(InboundMessage{UserID: "u", SessionID: "s", Channel: "web", ChatID: "c", Content: "hi"}); err != nil {
		t.Fatalf("PostInbound() error = %v", err)
	}
	if err := client.PostOutboundChunk(OutboundChunk{Channel: "web", ChatID: "c", Content: "x"}); err != nil {
		t.Fatalf("PostOutboundChunk() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := client.subscribeSSE(ctx, "/api/v1/outbound/stream?channel=web", "")
	if err != nil {
		t.Fatalf("subscribeSSE() error = %v", err)
	}
	_ = events

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("expected 3 paths to be hit, got %v", seen)
	}
	for path, header := range seen {
		if header != "Bearer s3cret" {
			t.Errorf("%s: Authorization = %q, want the configured token", path, header)
		}
	}
}

// Without an option there is nothing to send; the bus rejects an unauthenticated
// caller and this client must not pretend otherwise.
func TestClient_WithoutTokenSendsNoAuthorization(t *testing.T) {
	var header string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(Message{Type: "ok"})
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	if err := client.PostInbound(InboundMessage{UserID: "u", SessionID: "s", Channel: "web", ChatID: "c", Content: "hi"}); err != nil {
		t.Fatalf("PostInbound() error = %v", err)
	}
	if header != "" {
		t.Errorf("Authorization = %q, want none", header)
	}
}

func TestClient_PostInbound(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		return &Message{Type: "ok"}
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	err = client.PostInbound(InboundMessage{
		UserID:    "alice",
		SessionID: "cli:alice",
		Channel:   "cli",
		ChatID:    "alice",
		SenderID:  "alice",
		MessageID: "in-1",
		Content:   "Hello",
	})
	if err != nil {
		t.Errorf("PostInbound() error = %v", err)
	}
}

func TestClient_PollInbound_WithMessage(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		inbound := InboundMessage{
			UserID:    "alice",
			SessionID: "cli:alice",
			Channel:   "cli",
			ChatID:    "alice",
			SenderID:  "alice",
			MessageID: "in-2",
			Content:   "Test message",
			Timestamp: time.Now(),
		}
		resp, _ := NewMessage(TypeInbound, inbound)
		return &resp
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	msg, err := client.PollInbound("alice", 0, "agent-1")
	if err != nil {
		t.Fatalf("PollInbound() error = %v", err)
	}
	if msg == nil {
		t.Fatal("PollInbound() returned nil message")
	}
	if msg.Content != "Test message" {
		t.Errorf("Content = %q, want %q", msg.Content, "Test message")
	}
	if msg.MessageID != "in-2" {
		t.Errorf("MessageID = %q, want %q", msg.MessageID, "in-2")
	}
}

func TestClient_PostOutbound(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		return &Message{Type: "ok"}
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	err = client.PostOutbound(OutboundMessage{
		UserID:    "alice",
		SessionID: "cli:alice",
		Channel:   "cli",
		ChatID:    "alice",
		MessageID: "out-1",
		Content:   "Response",
	})
	if err != nil {
		t.Errorf("PostOutbound() error = %v", err)
	}
}

func TestClient_PollOutbound_WithMessage(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		outbound := OutboundMessage{
			UserID:    "alice",
			SessionID: "cli:alice",
			Channel:   "cli",
			ChatID:    "alice",
			MessageID: "out-2",
			Content:   "Outbound test",
		}
		resp, _ := NewMessage(TypeOutbound, outbound)
		return &resp
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	msg, err := client.PollOutbound("cli", "gw-1")
	if err != nil {
		t.Fatalf("PollOutbound() error = %v", err)
	}
	if msg == nil {
		t.Fatal("PollOutbound() returned nil message")
	}
	if msg.Content != "Outbound test" {
		t.Errorf("Content = %q, want %q", msg.Content, "Outbound test")
	}
	if msg.MessageID != "out-2" {
		t.Errorf("MessageID = %q, want %q", msg.MessageID, "out-2")
	}
}

func TestClient_Close(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message { return nil })
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	client.Close()
}

func TestClient_GetWeChatKFSyncState(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		resp, _ := NewMessage(TypeWeChatKFSyncState, WeChatKFSyncStateResponse{
			Found:          true,
			OpenKfID:       "kf_123",
			Cursor:         "cursor-1",
			CallbackToken:  "token-1",
			TokenExpiresAt: time.Now().Add(5 * time.Minute).UTC().Truncate(time.Second),
			UpdatedAt:      time.Now().UTC().Truncate(time.Second),
		})
		return &resp
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	state, err := client.GetWeChatKFSyncState("kf_123")
	if err != nil {
		t.Fatalf("GetWeChatKFSyncState() error = %v", err)
	}
	if state == nil {
		t.Fatal("GetWeChatKFSyncState() returned nil state")
	}
	if state.Cursor != "cursor-1" {
		t.Errorf("Cursor = %q, want %q", state.Cursor, "cursor-1")
	}
}

func TestClient_GetWeChatKFSyncState_NotFound(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		resp, _ := NewMessage(TypeWeChatKFSyncState, WeChatKFSyncStateResponse{Found: false})
		return &resp
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	state, err := client.GetWeChatKFSyncState("kf_123")
	if err != nil {
		t.Fatalf("GetWeChatKFSyncState() error = %v", err)
	}
	if state != nil {
		t.Fatalf("GetWeChatKFSyncState() = %#v, want nil", state)
	}
}

func TestClient_UpdateWeChatKFCursor(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		return &Message{Type: "ok"}
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	if err := client.UpdateWeChatKFCursor("kf_123", "cursor-2"); err != nil {
		t.Fatalf("UpdateWeChatKFCursor() error = %v", err)
	}
}

func TestClient_UpdateWeChatKFCallbackToken(t *testing.T) {
	ts := mockBusServer(t, func(msg *Message) *Message {
		return &Message{Type: "ok"}
	})
	defer ts.Close()

	client, err := NewClient(ts.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	if err := client.UpdateWeChatKFCallbackToken("kf_123", "token-2", time.Now().Add(10*time.Minute)); err != nil {
		t.Fatalf("UpdateWeChatKFCallbackToken() error = %v", err)
	}
}
