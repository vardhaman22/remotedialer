package remotedialer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rancher/remotedialer/metrics"
	"github.com/sirupsen/logrus"
)

var (
	Token = "X-API-Tunnel-Token"
	ID    = "X-API-Tunnel-ID"
)

func (s *Server) AddPeer(url, id, token string) {
	if s.PeerID == "" || s.PeerToken == "" {
		logrus.Info("AddPeer skipped: PeerID or PeerToken is empty")
		return
	}

	logrus.Infof("AddPeer called with url=%s id=%s token=%s", url, id, token)
	logrus.Infof("Server PeerID=%s PeerToken=%s", s.PeerID, s.PeerToken)

	ctx, cancel := context.WithCancel(context.Background())
	peer := peer{
		url:    url,
		id:     id,
		token:  token,
		cancel: cancel,
	}

	logrus.Infof("Adding peer entry for url=%s id=%s", url, id)

	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	if p, ok := s.peers[id]; ok {
		logrus.Infof("Existing peer found for id=%s", id)
		if p.equals(peer) {
			logrus.Infof("Peer config unchanged for id=%s, skipping restart", id)
			return
		}
		logrus.Infof("Peer config changed for id=%s, cancelling old peer", id)
		p.cancel()
	}

	s.peers[id] = peer
	logrus.Infof("Starting peer goroutine for id=%s", id)
	go peer.start(ctx, s)
}

func (s *Server) RemovePeer(id string) {
	logrus.Infof("RemovePeer called for id=%s", id)

	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	if p, ok := s.peers[id]; ok {
		logrus.Infof("Cancelling peer id=%s", id)
		p.cancel()
	}
	delete(s.peers, id)
	logrus.Infof("Peer id=%s removed from map", id)
}

type peer struct {
	url, id, token string
	cancel         func()
}

func (p peer) equals(other peer) bool {
	return p.url == other.url &&
		p.id == other.id &&
		p.token == other.token
}

func (p *peer) start(ctx context.Context, s *Server) {
	headers := http.Header{
		ID:    {s.PeerID},
		Token: {s.PeerToken},
	}

	logrus.Infof("Peer start loop initiated for id=%s url=%s", p.id, p.url)
	logrus.Infof("Request headers for peer id=%s: %v", p.id, headers)

	dialer := &websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		HandshakeTimeout: HandshakeTimeOut,
	}

	logrus.Infof("Websocket dialer created for peer id=%s (HandshakeTimeout=%v, InsecureSkipVerify=true)", p.id, HandshakeTimeOut)

	ctx = context.WithValue(ctx, ContextKeyCaller, fmt.Sprintf("Peer url:%s, id:%s", p.url, p.id))

outer:
	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Context cancelled for peer id=%s. Exiting loop.", p.id)
			break outer
		default:
		}

		logrus.Infof("Attempting websocket dial to url=%s for peer id=%s", p.url, p.id)
		metrics.IncSMTotalAddPeerAttempt(p.id)

		ws, resp, err := dialer.Dial(p.url, headers)
		if err != nil {
			if resp != nil {
				logrus.Infof("Dial failed for peer id=%s, HTTP status=%s", p.id, resp.Status)
			}
			logrus.Errorf("Failed to connect to peer url=%s localID=%s error=%v", p.url, s.PeerID, err)
			logrus.Infof("Sleeping 5 seconds before retry for peer id=%s", p.id)
			time.Sleep(5 * time.Second)
			continue
		}

		if resp != nil {
			logrus.Infof("Dial successful for peer id=%s, HTTP status=%s", p.id, resp.Status)
		}

		logrus.Infof("Websocket connection established for peer id=%s", p.id)
		metrics.IncSMTotalPeerConnected(p.id)

		session := NewClientSession(func(string, string) bool { return true }, ws)
		logrus.Infof("Client session created for peer id=%s", p.id)

		session.dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
			logrus.Infof("Session dialer invoked for peer id=%s network=%s address=%s", p.id, network, address)

			parts := strings.SplitN(network, "::", 2)
			if len(parts) != 2 {
				logrus.Infof("Invalid network format received: %s", network)
				return nil, fmt.Errorf("invalid clientKey/proto: %s", network)
			}

			clientKey := parts[0]
			proto := parts[1]

			logrus.Infof("Parsed clientKey=%s proto=%s for peer id=%s", clientKey, proto, p.id)

			d := s.Dialer(clientKey)
			logrus.Infof("Invoking server dialer for clientKey=%s proto=%s address=%s", clientKey, proto, address)

			return d(ctx, proto, address)
		}

		logrus.Infof("Adding session listener for peer id=%s", p.id)
		s.sessions.addListener(session)

		logrus.Infof("Starting session serve loop for peer id=%s", p.id)
		_, err = session.Serve(ctx)

		logrus.Infof("Session serve exited for peer id=%s err=%v", p.id, err)

		s.sessions.removeListener(session)
		logrus.Infof("Session listener removed for peer id=%s", p.id)

		session.Close()
		logrus.Infof("Session closed for peer id=%s", p.id)

		if err != nil {
			logrus.Errorf("Failed to serve peer connection %s: %v", p.id, err)
		}

		ws.Close()
		logrus.Infof("Websocket closed for peer id=%s", p.id)

		logrus.Infof("Sleeping 5 seconds before reconnect for peer id=%s", p.id)
		time.Sleep(5 * time.Second)
	}
}
