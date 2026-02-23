package remotedialer

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	errFailedAuth       = errors.New("failed authentication")
	errWrongMessageType = errors.New("wrong websocket message type")
)

type Authorizer func(req *http.Request) (clientKey string, authed bool, err error)
type ErrorWriter func(rw http.ResponseWriter, req *http.Request, code int, err error)

func DefaultErrorWriter(rw http.ResponseWriter, req *http.Request, code int, err error) {
	logrus.Infof("DefaultErrorWriter invoked: method=%s url=%s code=%d error=%v",
		req.Method, req.URL.String(), code, err)

	logrus.Infof("Request headers: %v", req.Header)
	rw.WriteHeader(code)
	rw.Write([]byte(err.Error()))
}

type Server struct {
	PeerID                  string
	PeerToken               string
	ClientConnectAuthorizer ConnectAuthorizer
	authorizer              Authorizer
	errorWriter             ErrorWriter
	sessions                *sessionManager
	peers                   map[string]peer
	peerLock                sync.Mutex
}

func New(auth Authorizer, errorWriter ErrorWriter) *Server {
	logrus.Info("Creating new remotedialer Server instance")

	return &Server{
		peers:       map[string]peer{},
		authorizer:  auth,
		errorWriter: errorWriter,
		sessions:    newSessionManager(),
	}
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	logrus.Infof("ServeHTTP invoked: method=%s url=%s remoteAddr=%s",
		req.Method, req.URL.String(), req.RemoteAddr)

	logrus.Infof("Incoming request headers: %v", req.Header)

	clientKey, authed, peer, err := s.auth(req)

	logrus.Infof("Auth result -> clientKey=%s authed=%v peer=%v err=%v",
		clientKey, authed, peer, err)

	if err != nil {
		logrus.Infof("Authentication returned error for clientKey=%s: %v", clientKey, err)
		s.errorWriter(rw, req, 400, err)
		return
	}

	if !authed {
		logrus.Infof("Authentication failed for clientKey=%s", clientKey)
		s.errorWriter(rw, req, 401, errFailedAuth)
		return
	}

	logrus.Infof("Handling backend connection request for clientKey=%s peer=%v",
		clientKey, peer)

	upgrader := websocket.Upgrader{
		HandshakeTimeout: 5 * time.Second,
		CheckOrigin: func(r *http.Request) bool {
			logrus.Infof("CheckOrigin called for host=%s origin=%s",
				r.Host, r.Header.Get("Origin"))
			return true
		},
		Error: s.errorWriter,
	}

	logrus.Infof("Attempting websocket upgrade for clientKey=%s", clientKey)

	wsConn, err := upgrader.Upgrade(rw, req, nil)
	if err != nil {
		logrus.Infof("Websocket upgrade failed for clientKey=%s error=%v",
			clientKey, err)

		s.errorWriter(rw, req, 400,
			errors.Wrapf(err, "Error during upgrade for host [%v]", clientKey))
		return
	}

	logrus.Infof("Websocket upgrade successful for clientKey=%s", clientKey)

	session := s.sessions.add(clientKey, wsConn, peer)
	logrus.Infof("Session created and added for clientKey=%s peer=%v",
		clientKey, peer)

	session.auth = s.ClientConnectAuthorizer
	defer func() {
		logrus.Infof("Removing session for clientKey=%s", clientKey)
		s.sessions.remove(session)
	}()

	logrus.Infof("Starting session Serve() for clientKey=%s", clientKey)

	code, err := session.Serve(req.Context())

	logrus.Infof("Session Serve() exited for clientKey=%s code=%d err=%v",
		clientKey, code, err)

	if err != nil {
		// Hijacked so we can't write to the client
		logrus.Infof("Error in remotedialer server session clientKey=%s code=%d err=%v",
			clientKey, code, err)
	}
}

func (s *Server) ListClients() []string {
	clients := s.sessions.listClients()
	logrus.Infof("ListClients called. Active clients: %v", clients)
	return clients
}

func (s *Server) auth(req *http.Request) (clientKey string, authed, peer bool, err error) {
	logrus.Infof("auth() invoked for remoteAddr=%s", req.RemoteAddr)

	id := req.Header.Get(ID)
	token := req.Header.Get(Token)

	logrus.Infof("Auth headers -> ID=%s Token=%s", id, token)

	if id != "" && token != "" {
		logrus.Infof("Attempting peer authentication for id=%s", id)

		s.peerLock.Lock()
		p, ok := s.peers[id]
		s.peerLock.Unlock()

		if !ok {
			logrus.Infof("Peer id=%s not found in server peer map", id)
		} else {
			logrus.Infof("Peer id=%s found. Stored token=%s", id, p.token)
		}

		if ok && p.token == token {
			logrus.Infof("Peer authentication successful for id=%s", id)
			return id, true, true, nil
		}

		logrus.Infof("Peer authentication failed for id=%s", id)
	}

	logrus.Infof("Falling back to client authorizer")

	id, authed, err = s.authorizer(req)

	logrus.Infof("Client authorizer result -> id=%s authed=%v err=%v",
		id, authed, err)

	return id, authed, false, err
}