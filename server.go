package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

var (
	value int
	mu    sync.RWMutex
)

func getValueHandler(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	resp := map[string]int{"value": value}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func setValueHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var data struct {
		Value int `json:"value"`
	}

	if err := json.Unmarshal(body, &data); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	mu.Lock()
	value = data.Value
	mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func main() {

	// -------------------------------
	// ARGUMENT PARSING
	// -------------------------------
	if len(os.Args) != 4 {
		fmt.Println("Usage: go run server.go <myRpcPort> <myHttpPort> <peerRpcPort>")
		fmt.Println("Example: go run server.go 8081 8080 8082")
		return
	}

	myRpcPort := os.Args[1]
	myHttpPort := os.Args[2]
	peerRpcPort := os.Args[3]

	peerAddress := "localhost:" + peerRpcPort

	// -------------------------------
	// CREATE SERVER
	// -------------------------------
	srv := &Server{
		CurrentState: "follower",
		CurrentTerm:  0,
		heartbeatCh:  make(chan struct{}, 1),
		Peers:        []string{peerAddress},
	}

	rpc.Register(srv)

	// -------------------------------
	// START RPC SERVER
	// -------------------------------
	go func() {
		addr := ":" + myRpcPort
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("RPC server on", addr)
		rpc.Accept(listener) // blocks forever
	}()

	// -------------------------------
	// START ELECTION TIMEOUT LOOP
	// -------------------------------
	go srv.electionTimeoutLoop()

	// -------------------------------
	// START HTTP API SERVER
	// -------------------------------
	http.HandleFunc("/value", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			getValueHandler(w, r)
			return
		}
		if r.Method == http.MethodPost {
			setValueHandler(w, r)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	httpAddr := ":" + myHttpPort

	log.Println("HTTP server running at http://localhost" + httpAddr)
	log.Fatal(http.ListenAndServe(httpAddr, nil))
}
