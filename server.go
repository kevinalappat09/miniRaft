package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Server struct {
	id    int
	port  string
	peers []string
	raft  *Raft

	// the actual variable that we want to replicate
	value int
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
	// candidates term
	Term int
	// candidte id
	Id int
	// index of candidates last log
	LastLogIndex int
	// term of candidates last log
	LastLogTerm int
}

type RequestVotesReply struct {
	// term for candidate to update
	Term int
	// true means candidate received the vote
	VoteGranted bool
}

type ChangeValueArgs struct {
	Value int
}

type ChangeValueReply struct {
	Success bool
}

type GetValueArgs struct{}

type GetValueReply struct {
	Value int
}

func (s *Server) Start(_ struct{}, _ *struct{}) error {
	go s.raft.runElectionTimer()
	return nil
}

func (s *Server) ChangeValue(args ChangeValueArgs, reply *ChangeValueReply) error {
	fmt.Printf("[server %d] received request from client:  %d\n", s.id, args.Value)
	reply.Success = s.raft.handleCommand(args.Value)
	return nil
}

func (s *Server) GetValue(args GetValueArgs, reply *GetValueReply) error {
	fmt.Printf("[server %d] received get value : %d", s.id, s.value)
	reply.Value = s.value
	return nil
}

func main() {
	port := os.Args[1]
	possible_peers := []string{":8080", ":8081", ":8082"}

	server := new(Server)
	raft := new(Raft)

	numstr := port[1:]
	num, _ := strconv.Atoi(numstr)
	server.id = num % 10
	server.peers = make([]string, 0)
	for _, peer := range possible_peers {
		if peer == port {
			continue
		}
		server.peers = append(server.peers, peer)
	}
	server.raft = raft
	server.raft.InitializeRaft(server)

	rpc.Register(server)
	rpc.RegisterName("Raft", server.raft)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Server init : %v", err)
	}
	fmt.Printf("Server running on port %s\n", port)
	http.Serve(listener, nil)
}
