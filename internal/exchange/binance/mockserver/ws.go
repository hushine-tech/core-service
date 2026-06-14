package mockserver

import (
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

type wsClient struct {
	listenKey string
	send      chan []byte
}

func (s *Server) registerWSRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/ws/", s.handleWebSocket)
}

func (s *Server) EmitFuturesOrderEvent(event BinanceOrderEvent) {
	s.broadcast(FuturesOrderTradeUpdate(event))
}

func (s *Server) EmitSpotOrderEvent(event BinanceOrderEvent) {
	s.broadcast(SpotExecutionReport(event))
}

func (s *Server) broadcast(payload []byte) {
	s.mu.Lock()
	clients := make([]*wsClient, 0)
	for _, byClient := range s.listenKeys {
		for client := range byClient {
			clients = append(clients, client)
		}
	}
	s.mu.Unlock()

	for _, client := range clients {
		select {
		case client.send <- payload:
		default:
		}
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	listenKey := strings.TrimPrefix(r.URL.Path, "/ws/")
	if strings.TrimSpace(listenKey) == "" {
		http.Error(w, "missing listenKey", http.StatusBadRequest)
		return
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{listenKey: listenKey, send: make(chan []byte, 16)}
	s.addClient(client)
	defer func() {
		s.removeClient(client)
		_ = conn.Close()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case payload := <-client.send:
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		}
	}
}

func (s *Server) addClient(client *wsClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listenKeys[client.listenKey] == nil {
		s.listenKeys[client.listenKey] = make(map[*wsClient]struct{})
	}
	s.listenKeys[client.listenKey][client] = struct{}{}
}

func (s *Server) removeClient(client *wsClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clients := s.listenKeys[client.listenKey]; clients != nil {
		delete(clients, client)
	}
}
