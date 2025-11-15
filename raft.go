package main

import (
	"log"
	"math/rand/v2"
	"net/rpc"
	"sync"
	"time"
)

// struct storing server metadata
type Server struct {
	CurrentState string
	CurrentTerm  int

	// channel for incoming heartbeats
	heartbeatCh chan struct{}

	Peers []string
}

type AppendEntriesArgs struct {
	Term  int
	Index int
	Value int
}

type AppendEntriesReply struct {
	Success bool
}

// RPC function that gets heartbeats from other servers.
func (s *Server) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	// send an empty struct to heartbeat channel
	// but if the channel is already full then do not block
	select {
	case s.heartbeatCh <- struct{}{}:
	default:
	}

	if args.Value == -1 {
		log.Printf("Heartbeat received. Current Term : %d, Current Index : %d", args.Term, args.Index)
	}

	// dummy reply for now.
	reply.Success = true
	return nil
}

type RequestVotesArgs struct {
	Term int
}

type RequestVotesReply struct {
	Vote bool
}

// receives the RequestVotes RPC from the candidate
// returns false if the candidate term < servers term
// returns true otherwise, sets current term to candidate term.
func (s *Server) RequestVotes(args RequestVotesArgs, reply *RequestVotesReply) error {
	if args.Term < s.CurrentTerm {
		reply.Vote = false
		return nil
	}
	if args.Term > s.CurrentTerm {
		s.CurrentTerm = args.Term
		s.CurrentState = "follower"
	}

	reply.Vote = true
	return nil
}

// random timeout to start election when no heartbeat received.
func (s *Server) electionTimeoutLoop() {
	for {
		if s.CurrentState != "follower" {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		timeout := time.Duration(150+rand.IntN(150)) * time.Millisecond
		timer := time.NewTimer(timeout)

		select {
		// if we get the heartbeat then reset the timer
		case <-s.heartbeatCh:
			// if stop returns false then it means the timer
			// already fired, we read the channel value to empty it.
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			log.Println("Election timeout. Becoming candidate.")
			s.becomeCandidate()
		}
	}
}

func (s *Server) becomeCandidate() {
	// set it's state to candidate
	s.CurrentState = "candidate"
	s.CurrentTerm++

	log.Printf("Becoming a candidate for term %d\n", s.CurrentTerm)

	// vote for itself
	votes := 1
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			var reply RequestVotesReply
			args := RequestVotesArgs{
				Term: s.CurrentTerm,
			}
			err := callRequestVote(peerAddr, args, &reply)
			if err == nil && reply.Vote {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peer)
	}

	wg.Wait()
	if votes > (len(s.Peers)+1)/2 {
		log.Printf("Won election with %d votes — becoming leader", votes)
		s.becomeLeader()
	} else {
		log.Printf("Lost election with %d votes — returning to follower", votes)
		s.CurrentState = "follower"
	}
}

func (s *Server) becomeLeader() {
	s.CurrentState = "leader"
	log.Println("I am leader")
	s.leaderHeartbeatLoop()
}

func (s *Server) leaderHeartbeatLoop() {
	for s.CurrentState == "leader" {
		for _, peer := range s.Peers {
			go func(peerAddr string) {
				var reply AppendEntriesReply
				args := AppendEntriesArgs{
					Term:  s.CurrentTerm,
					Index: 0,
					Value: -1,
				}
				callAppendEntries(peer, args, &reply)
			}(peer)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func callAppendEntries(addr string, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}

	defer client.Close()

	return client.Call("Server.AppendEntries", args, reply)
}

func callRequestVote(addr string, args RequestVotesArgs, reply *RequestVotesReply) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Call("Server.RequestVotes", args, reply)
}
