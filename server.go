package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Server struct {
	id    int
	port  string
	peers []string
	raft  *Raft
}

type AppendEntriesArgs struct {
	// Leaders term
	Term int
	// The id of the leader
	LeaderId int
	// index of log entry immediately preceeding the new one
	PreviousLogIndex int
	// term of the previous log
	PreviousLogTerm int
	// Log entry, only sends one for simplicity
	Entry LogEntry
	// Leaders commit index
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	// current term of the follower for leader to update itself
	Term int
	// boolean success
	Success bool
}

type RequestVotesArgs struct {
}

type RequestVotesReply struct {
}

func main() {
	port := os.Args[1]
	possible_peers := []string{":8080", ":8081", ":8082"}

	server := new(Server)
	raft := new(Raft)

	server.peers = make([]string, 0)
	for _, peer := range possible_peers {
		if peer == port {
			continue
		}
		server.peers = append(server.peers, peer)
	}
	server.raft = raft

	raft.server = server
	raft.currentTerm = 0

	rpc.Register(server)
	rpc.Register(server.raft)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Server init : %v", err)
	}
	fmt.Printf("Server running on port %s\n", port)
	http.Serve(listener, nil)
}
