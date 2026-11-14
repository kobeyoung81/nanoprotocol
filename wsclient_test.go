package nanoprotocol

// The agent socket is an optimisation, so these tests pin the two properties that
// make it safe to rely on: the request/response envelope behaves exactly as it
// does over HTTP, and the pushes are wake-ups that cannot lose a message.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// socketServer is a stand-in bus: it registers sockets, answers requests on the
// socket, records what arrived over which transport, and can push.
type socketServer struct {
	t  *testing.T
	up websocket.Upgrader

	mu          sync.Mutex
	registered  []AgentSocketRegister
	socketCalls []string // message types that arrived over the socket
	httpCalls   []string // message types that arrived over POST /api/v1/rpc
	conns       []*websocket.Conn
	closeNext   bool
}

func newSocketServer(t *testing.T) (*socketServer, *httptest.Server) {
	t.Helper()
	s := &socketServer{t: t, up: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws/agent", s.handleSocket)
	mux.HandleFunc("POST /api/v1/rpc", s.handleRPC)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return s, srv
}

func (s *socketServer) handleSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	var reg Message
	if err := conn.ReadJSON(&reg); err != nil {
		return
	}
	var payload AgentSocketRegister
	if err := reg.DecodePayload(&payload); err != nil {
		return
	}
	s.mu.Lock()
	s.registered = append(s.registered, payload)
	s.conns = append(s.conns, conn)
	closeNext := s.closeNext
	s.closeNext = false
	s.mu.Unlock()

	ack, _ := NewMessage(TypeAgentSocketRegistered, struct{}{})
	if err := conn.WriteJSON(ack); err != nil {
		return
	}
	if closeNext {
		// One-shot: the client must notice and come back.
		return
	}

	for {
		var msg Message
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		s.mu.Lock()
		s.socketCalls = append(s.socketCalls, msg.Type)
		s.mu.Unlock()
		resp := Message{Type: "ok"}
		if err := conn.WriteJSON(resp); err != nil {
			return
		}
	}
}

func (s *socketServer) handleRPC(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.httpCalls = append(s.httpCalls, msg.Type)
	s.mu.Unlock()
	_, _ = w.Write([]byte(`{"type":"ok"}`))
}

// dropCurrent closes the live socket from the server side.
func (s *socketServer) dropCurrent() {
	s.mu.Lock()
	conns := append([]*websocket.Conn(nil), s.conns...)
	s.conns = nil
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (s *socketServer) current() *websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.conns) == 0 {
		return nil
	}
	return s.conns[len(s.conns)-1]
}

func (s *socketServer) counts() (sockets, https int, regs []AgentSocketRegister) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.socketCalls), len(s.httpCalls), append([]AgentSocketRegister(nil), s.registered...)
}

func TestDialWSRegistersAndCarriesRequestsOnTheSocket(t *testing.T) {
	s, srv := newSocketServer(t)

	client, err := DialWS(srv.URL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	if client.SocketConnected() {
		t.Fatal("a socket is not usable before registration")
	}
	if err := client.RegisterAgentSocket(AgentSocketRegister{UserID: "user-1", AgentID: "agent-1"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !client.SocketConnected() {
		t.Fatal("expected the socket to be the request path after registration")
	}

	msg, err := NewMessage(TypePollInbound, PollInboundRequest{UserID: "user-1", AgentID: "agent-1"})
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	if _, err := client.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	sockets, https, regs := s.counts()
	if sockets != 1 || https != 0 {
		t.Fatalf("expected the request on the socket only, got %d socket / %d http", sockets, https)
	}
	if len(regs) != 1 || regs[0].UserID != "user-1" || regs[0].AgentID != "agent-1" {
		t.Fatalf("unexpected registrations: %+v", regs)
	}
}

func TestDialWSDeliversPushesAsWakeUpsAndNotifications(t *testing.T) {
	s, srv := newSocketServer(t)

	client, err := DialWS(srv.URL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	if err := client.RegisterAgentSocket(AgentSocketRegister{
		UserID: "user-1", AgentID: "agent-1", Components: []string{SocketComponentConfig},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	conn := s.current()
	if conn == nil {
		t.Fatal("no server-side connection")
	}
	ready, _ := NewMessage(TypeInboundReady, InboundReady{Count: 1})
	if err := conn.WriteJSON(ready); err != nil {
		t.Fatalf("push inbound_ready: %v", err)
	}
	change, _ := NewMessage(TypePlatformEvent, PlatformEventNotice{
		Type:    TypeConfigChanged,
		Content: `{"agent_id":"","keys":["agent.default"]}`,
	})
	if err := conn.WriteJSON(change); err != nil {
		t.Fatalf("push platform_event: %v", err)
	}

	select {
	case <-client.InboundReady():
	case <-time.After(2 * time.Second):
		t.Fatal("no inbound_ready wake-up")
	}
	select {
	case got := <-client.ConfigChanges():
		if len(got.Keys) != 1 || got.Keys[0] != "agent.default" {
			t.Fatalf("unexpected change: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no config change")
	}
}

// A push that arrives while no request is in flight must not be mistaken for a
// response, and must not consume the next request's answer either.
func TestDialWSPushDoesNotStealTheNextResponse(t *testing.T) {
	s, srv := newSocketServer(t)

	client, err := DialWS(srv.URL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	if err := client.RegisterAgentSocket(AgentSocketRegister{UserID: "u", AgentID: "a"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	ready, _ := NewMessage(TypeInboundReady, InboundReady{})
	if err := s.current().WriteJSON(ready); err != nil {
		t.Fatalf("push: %v", err)
	}
	msg, _ := NewMessage(TypePollInbound, PollInboundRequest{UserID: "u", AgentID: "a"})
	resp, err := client.Send(msg)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if resp == nil || resp.Type != "ok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestDialWSReconnectsAndRegistersAgain(t *testing.T) {
	s, srv := newSocketServer(t)

	client, err := DialWS(srv.URL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	if err := client.RegisterAgentSocket(AgentSocketRegister{UserID: "user-1", AgentID: "agent-1"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.dropCurrent()

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, regs := s.counts()
		if len(regs) >= 2 && client.SocketConnected() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the socket did not come back: registrations=%d connected=%v", len(regs), client.SocketConnected())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Until the socket is registered — and whenever it is down — requests take the
// HTTP path, so nothing depends on it being up.
func TestRequestsFallBackToHTTPWithoutASocket(t *testing.T) {
	s, srv := newSocketServer(t)

	client, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()
	if client.SocketConnected() {
		t.Fatal("a plain client has no socket")
	}
	msg, _ := NewMessage(TypePollInbound, PollInboundRequest{UserID: "u"})
	if _, err := client.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	sockets, https, _ := s.counts()
	if sockets != 0 || https != 1 {
		t.Fatalf("expected the HTTP path, got %d socket / %d http", sockets, https)
	}
}

func TestDialWSPointsAtTheAgentSocketPath(t *testing.T) {
	c, err := NewClient("http://127.0.0.1:1/prefix")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	wc, err := c.dialSocket()
	if err == nil {
		_ = wc
		t.Fatal("dialing a host that does not exist must fail")
	}
	if !strings.Contains(err.Error(), "dial agent socket") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(strings.TrimRight(c.baseURL, "/"), "/prefix") {
		t.Fatalf("the base url must be left alone: %s", c.baseURL)
	}
}
