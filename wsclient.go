package nanoprotocol

// The agent socket.
//
// An agent used to ask the bus for work every 500 ms: a full HTTP round trip per
// poll, of which roughly eighty percent of the bytes are headers. The bus
// already speaks the same message envelope over a WebSocket (its catch-all
// handler dispatches to the same handlers as /api/v1/rpc), so the agent can run
// those requests on one connection instead, and the bus can push the two things
// an agent waits for: an inbound message arriving, and a platform notification
// (configuration changed, an agent record changed).
//
// The pushes carry no state. inbound_ready only says "a message is waiting";
// the agent still claims it with poll_inbound, which is the atomic step, so a
// duplicated push is a wasted query and a lost push is covered by the fallback
// poll. platform_event forwards the same JSON payload the SSE stream carries.
//
// The socket is an optimisation, never a dependency: until it is registered and
// whenever it is down, Send falls back to POST /api/v1/rpc and the notification
// channels are served by the SSE streams the client already had.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// AgentSocketPath is where an agent process connects.
	AgentSocketPath = "/ws/agent"

	// agentSocketWriteTimeout bounds one frame write. Same reasoning as the
	// hypervisor tunnel: a half-dead socket must not block a caller forever.
	agentSocketWriteTimeout = 10 * time.Second
	// agentSocketRequestTimeout bounds one request/response exchange.
	agentSocketRequestTimeout = 60 * time.Second
	// agentSocketIdleTimeout is the read deadline: the bus pings every 30 s, so
	// three missed pings mean the connection is gone even without an error.
	agentSocketIdleTimeout = 90 * time.Second
	// agentSocketPushBuffer is how far a push consumer may fall behind before
	// the notification is dropped. Everything dropped is re-derivable.
	agentSocketPushBuffer = 16
	// Reconnect backoff bounds.
	agentSocketMinBackoff = time.Second
	agentSocketMaxBackoff = 30 * time.Second
)

// agentSocketConn is one live connection. It is replaced on reconnect, so
// anything that outlives a connection (the push channels, the request lock)
// lives on the Client instead.
type agentSocketConn struct {
	conn      *websocket.Conn
	resp      chan *Message
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *agentSocketConn) shutdown() {
	c.closeOnce.Do(func() { close(c.closed) })
}

// DialWS opens the agent socket and returns a client whose requests run on it
// once it is registered. The URL may be ws://, wss://, http:// or https:// — the
// path is added, so a caller passes the same --bus-url it always did.
//
// The socket is not registered here: RegisterAgentSocket binds the connection to
// a user and an agent, and until that succeeds every request goes over HTTP.
func DialWS(rawURL string, opts ...Option) (*Client, error) {
	c, err := NewClient(rawURL, opts...)
	if err != nil {
		return nil, err
	}
	c.configCh = make(chan ConfigChange, agentSocketPushBuffer)
	c.agentsCh = make(chan AgentChangeNotification, agentSocketPushBuffer)
	c.inboundReady = make(chan struct{}, 1)

	wc, err := c.dialSocket()
	if err != nil {
		return nil, err
	}
	c.setSocketConn(wc)
	go c.wsReadLoop(wc)
	return c, nil
}

// RegisterAgentSocket tells the bus who is on this connection, and keeps it that
// way: from here on the client reconnects and re-registers on its own.
func (c *Client) RegisterAgentSocket(reg AgentSocketRegister) error {
	c.wsRegister = &reg
	if err := c.registerOnCurrentSocket(); err != nil {
		return err
	}
	c.wsSuperviseOnce.Do(func() { go c.wsSupervise() })
	return nil
}

// SocketConnected reports whether requests are running on the socket right now.
// A client built with NewClient always answers false: it has no socket.
func (c *Client) SocketConnected() bool { return c.wsReady.Load() }

// InboundReady fires when the bus says a message is waiting for this agent. The
// channel coalesces: one pending signal is enough, since the answer is always
// "poll now". It is nil on a client without a socket, which makes `select` on it
// block forever.
func (c *Client) InboundReady() <-chan struct{} { return c.inboundReady }

// registerOnCurrentSocket performs the registration exchange on the live
// connection and only then routes requests through it.
func (c *Client) registerOnCurrentSocket() error {
	wc := c.socketConn()
	if wc == nil {
		return fmt.Errorf("register agent socket: not connected")
	}
	if c.wsRegister == nil {
		return fmt.Errorf("register agent socket: no registration")
	}
	msg, err := NewMessage(TypeAgentSocketRegister, *c.wsRegister)
	if err != nil {
		return err
	}
	if err := c.writeSocket(wc, msg); err != nil {
		return fmt.Errorf("register agent socket: %w", err)
	}
	resp, err := c.awaitResponse(wc, agentSocketWriteTimeout)
	if err != nil {
		return fmt.Errorf("register agent socket: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("register agent socket: no response")
	}
	if resp.Type == TypeError {
		return fmt.Errorf("register agent socket: %s", resp.Error)
	}
	c.wsReady.Store(true)
	return nil
}

// wsSupervise reconnects and re-registers after a connection dies, with
// exponential backoff. While it is down SocketConnected is false and requests
// take the HTTP path, so nothing fails, it only costs a round trip.
func (c *Client) wsSupervise() {
	backoff := agentSocketMinBackoff
	for {
		wc := c.socketConn()
		if wc == nil {
			// A failed initial dial: try again from scratch.
			fresh, err := c.dialSocket()
			if err != nil {
				time.Sleep(backoff)
				backoff = growBackoff(backoff)
				continue
			}
			c.setSocketConn(fresh)
			go c.wsReadLoop(fresh)
			wc = fresh
		}
		<-wc.closed
		c.setSocketConn(nil)
		c.wsReady.Store(false)

		for {
			fresh, err := c.dialSocket()
			if err == nil {
				c.setSocketConn(fresh)
				go c.wsReadLoop(fresh)
				if err := c.registerOnCurrentSocket(); err == nil {
					backoff = agentSocketMinBackoff
					break
				}
				_ = fresh.conn.Close()
				fresh.shutdown()
				c.setSocketConn(nil)
			}
			time.Sleep(backoff)
			backoff = growBackoff(backoff)
		}
	}
}

func growBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > agentSocketMaxBackoff {
		return agentSocketMaxBackoff
	}
	return d
}

func (c *Client) dialSocket() (*agentSocketConn, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid bus url %q: %w", c.baseURL, err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("unsupported bus url %q", c.baseURL)
	}
	u.Path = strings.TrimRight(u.Path, "/") + AgentSocketPath

	header := http.Header{}
	if c.token != "" {
		header.Set("Authorization", "Bearer "+c.token)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := dialer.Dial(u.String(), header)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial agent socket: %w (http %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial agent socket: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(agentSocketIdleTimeout))
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(agentSocketIdleTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(agentSocketWriteTimeout))
	})
	return &agentSocketConn{
		conn:   conn,
		resp:   make(chan *Message, 1),
		closed: make(chan struct{}),
	}, nil
}

func (c *Client) socketConn() *agentSocketConn {
	c.wsStateMu.RLock()
	defer c.wsStateMu.RUnlock()
	return c.wsConn
}

func (c *Client) setSocketConn(wc *agentSocketConn) {
	c.wsStateMu.Lock()
	c.wsConn = wc
	c.wsStateMu.Unlock()
}

// wsSend runs one request/response exchange on the socket.
func (c *Client) wsSend(msg Message) (*Message, error) {
	wc := c.socketConn()
	if wc == nil {
		return c.sendHTTP(msg)
	}
	// One exchange at a time: the bus answers in order, and correlating by
	// arrival is what keeps the envelope unchanged (it has no request id).
	c.wsReqMu.Lock()
	defer c.wsReqMu.Unlock()

	// A response that arrived after its request timed out is not ours.
	select {
	case <-wc.resp:
	default:
	}

	if err := c.writeSocket(wc, msg); err != nil {
		c.wsReady.Store(false)
		wc.shutdown()
		return nil, fmt.Errorf("send %s: %w", msg.Type, err)
	}
	resp, err := c.awaitResponse(wc, agentSocketRequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("send %s: %w", msg.Type, err)
	}
	return resp, nil
}

func (c *Client) writeSocket(wc *agentSocketConn, msg Message) error {
	_ = wc.conn.SetWriteDeadline(time.Now().Add(agentSocketWriteTimeout))
	c.wsWriteMu.Lock()
	err := wc.conn.WriteJSON(msg)
	c.wsWriteMu.Unlock()
	_ = wc.conn.SetWriteDeadline(time.Time{})
	return err
}

func (c *Client) awaitResponse(wc *agentSocketConn, timeout time.Duration) (*Message, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case resp := <-wc.resp:
		return resp, nil
	case <-wc.closed:
		return nil, ErrConnectionClosed
	case <-timer.C:
		return nil, fmt.Errorf("no response from the bus")
	}
}

func (c *Client) wsReadLoop(wc *agentSocketConn) {
	for {
		var msg Message
		if err := wc.conn.ReadJSON(&msg); err != nil {
			wc.shutdown()
			return
		}
		_ = wc.conn.SetReadDeadline(time.Now().Add(agentSocketIdleTimeout))
		if c.wsDeliverPush(msg) {
			continue
		}
		select {
		case wc.resp <- &msg:
		default:
			// Nobody is waiting: the answer to a request that already timed out.
		}
	}
}

// wsDeliverPush routes the two push types and reports whether it consumed the
// message.
func (c *Client) wsDeliverPush(msg Message) bool {
	switch msg.Type {
	case TypeInboundReady:
		select {
		case c.inboundReady <- struct{}{}:
		default:
		}
		return true
	case TypePlatformEvent:
		var notice PlatformEventNotice
		if err := msg.DecodePayload(&notice); err != nil {
			return true
		}
		switch notice.Type {
		case TypeConfigChanged:
			var change ConfigChange
			if err := json.Unmarshal([]byte(notice.Content), &change); err != nil {
				return true
			}
			select {
			case c.configCh <- change:
			default:
			}
		case TypeAgentsChanged:
			var change AgentChangeNotification
			if err := json.Unmarshal([]byte(notice.Content), &change); err != nil {
				return true
			}
			select {
			case c.agentsCh <- change:
			default:
			}
		}
		return true
	}
	return false
}

// closeSocket closes the current connection, if any.
func (c *Client) closeSocket() {
	wc := c.socketConn()
	if wc == nil {
		return
	}
	c.wsReady.Store(false)
	_ = wc.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))
	_ = wc.conn.Close()
	wc.shutdown()
}
