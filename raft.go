package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type LogEntry struct {
	// in our case we simply pass the variable value
	Command int
	// term of the entry
	Term int
}

type Raft struct {
	// Mutex for mutual exclusion
	mu sync.Mutex

	// Reference to the underlying server (used to update the variable)
	server *Server

	// last term the server has seen.
	currentTerm int

	// candidate id that received vote in the current term
	// -1 if none
	votedFor int

	// log entries
	log []LogEntry

	// index of the highest log entry known to be committed
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int

	// for each server index of next log entry to send
	nextIndex []int
	// for each server, index of last log entry that has been applied
	matchIndex []int

	// indicates when the last election timeout reset occured
	electionResetTimeout time.Time
	// stores the state - Follower, Candidate, Leader
	state string
}

func (r *Raft) getLastLogInfo() (index int, term int) {
	// if no logs exist then return -1 and 0
	if len(r.log) == 0 {
		return -1, 0
	}
	last := len(r.log) - 1
	return last, r.log[last].Term
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("[Server %d] Received append entries from %d (term=%d)\n", r.server.id, args.LeaderId, args.Term)

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term

		// revert to Follower
		r.votedFor = -1
		r.state = "Follower"
	}
	r.electionResetTimeout = time.Now()

	reply.Term = r.currentTerm
	reply.Success = true
	return nil
}

func (r *Raft) callAppendEntries(peer string, args AppendEntriesArgs) (bool, *AppendEntriesReply) {
	client, err := rpc.DialHTTP("tcp", "localhost"+peer)
	if err != nil {
		fmt.Printf("[server %d] RPC dial to %s failed: %v\n", r.server.id, peer, err)
		return false, nil
	}
	defer client.Close()

	reply := &AppendEntriesReply{}
	err = client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		fmt.Printf("[server %d] AppendEntries RPC to %s failed: %v\n", r.server.id, peer, err)
		return false, nil
	}
	return true, reply
}

func (r *Raft) RequestVotes(args RequestVotesArgs, reply *RequestVotesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("RequestVotes received from server : %d\n", args.Id)
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		fmt.Println("\tRejected RequestVotes as current term higher")
		return nil
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = -1
		r.becomeFollower()
	}

	lastLogIndex, lastLogTerm := r.getLastLogInfo()
	logUpToDate := false

	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	if !logUpToDate {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		fmt.Println("\tRejected vote: log not up-to-date")
		return nil
	}

	if r.votedFor == -1 || r.votedFor == args.Id {
		r.votedFor = args.Id
		r.electionResetTimeout = time.Now()

		reply.Term = r.currentTerm
		reply.VoteGranted = true
		fmt.Println("\tAccepted vote")
		return nil
	}
	reply.Term = r.currentTerm
	reply.VoteGranted = false
	fmt.Println("\tRejected vote: already voted")

	return nil
}

func (r *Raft) callRequestVotes(peer string, args *RequestVotesArgs) (bool, *RequestVotesReply) {
	client, err := rpc.DialHTTP("tcp", "localhost"+peer)
	if err != nil {
		fmt.Printf("[server %d] RPC dial to %s failed: %v\n", r.server.id, peer, err)
		return false, nil
	}
	defer client.Close()

	reply := &RequestVotesReply{}
	err = client.Call("Raft.RequestVotes", args, reply)
	if err != nil {
		fmt.Printf("[server %d] RequestVotes RPC to %s failed: %v\n", r.server.id, peer, err)
		return false, nil
	}
	return true, reply
}

func (r *Raft) runElectionTimer() {
	for {
		timeout := getElectionTimeout()

		r.mu.Lock()
		// capture state values under lock for logging
		startTerm := r.currentTerm
		state := r.state
		// ensure we have a sensible last reset time (should be set on init)
		// startTime := r.electionResetTimeout
		r.mu.Unlock()

		fmt.Printf("[server %d] Election timer started (term=%d, state=%s) timeout=%v\n", r.server.id, startTerm, state, timeout)

		ticker := time.NewTicker(10 * time.Millisecond)
		fired := false

		for !fired {
			<-ticker.C

			r.mu.Lock()
			// if we've become leader, stop the timer loop entirely
			if r.state == "Leader" {
				r.mu.Unlock()
				ticker.Stop()
				fmt.Printf("[server %d] Stopping election timer because state=Leader\n", r.server.id)
				return
			}

			// If term changed since this timer started, break to restart fresh
			if startTerm != r.currentTerm {
				r.mu.Unlock()
				ticker.Stop()
				// restart outer loop (fresh timeout)
				break
			}

			// If timeout elapsed -> start an election
			if time.Since(r.electionResetTimeout) >= timeout {
				// we will start election outside the lock
				r.mu.Unlock()
				ticker.Stop()
				r.startElection()
				fired = true
				break
			}
			r.mu.Unlock()
		}

		// small sleep to avoid tight restart loops
		time.Sleep(10 * time.Millisecond)
	}
}

// Gets a random eletion timeout
func getElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (r *Raft) startElection() {
	// prepare candidate state
	r.mu.Lock()
	r.state = "Candidate"
	r.currentTerm++
	r.votedFor = r.server.id
	r.electionResetTimeout = time.Now()
	term := r.currentTerm
	lastIndex, lastTerm := r.getLastLogInfo()
	r.mu.Unlock()

	fmt.Printf("[server %d] Starting election for term %d\n", r.server.id, term)

	args := &RequestVotesArgs{
		Term:         term,
		Id:           r.server.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	votes := 1
	var muVotes sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range r.server.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			ok, reply := r.callRequestVotes(peer, args)
			if !ok || reply == nil {
				return
			}

			r.mu.Lock()
			// if reply has higher term, step down
			if reply.Term > r.currentTerm {
				r.currentTerm = reply.Term
				r.votedFor = -1
				r.state = "Follower"
				r.electionResetTimeout = time.Now()
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()

			if reply.VoteGranted {
				muVotes.Lock()
				votes++
				muVotes.Unlock()
				fmt.Printf("[server %d] got vote from %s (total=%d)\n", r.server.id, peer, votes)
			}
		}(peer)
	}

	wg.Wait()

	majority := len(r.server.peers)/2 + 1
	fmt.Printf("[server %d] Election finished in term %d; votes=%d (majority needed=%d)\n", r.server.id, term, votes, majority)

	r.mu.Lock()
	if r.currentTerm != term {
		r.mu.Unlock()
		return
	}

	if votes >= majority {
		r.state = "Leader"
		fmt.Printf("[server %d] BECAME LEADER for term %d\n", r.server.id, term)
		r.startHeartbeats()
		r.mu.Unlock()
		return
	}

	r.becomeFollower()
	r.mu.Unlock()
}

func (r *Raft) becomeFollower() {
	r.votedFor = -1
	r.electionResetTimeout = time.Now()
	r.state = "Follower"
	fmt.Println("Becoming Follower")
}

func (r *Raft) sendHeartbeats() {
	r.mu.Lock()
	if r.state != "Leader" {
		r.mu.Unlock()
		return
	}

	term := r.currentTerm
	r.mu.Unlock()

	for _, peer := range r.server.peers {
		go func(peer string) {
			args := &AppendEntriesArgs{
				Term:              term,
				LeaderId:          r.server.id,
				PreviousLogIndex:  -1,
				PreviousLogTerm:   0,
				Entry:             LogEntry{},
				LeaderCommitIndex: 0,
			}
			_, _ = r.callAppendEntries(peer, *args)
		}(peer)
	}
}

func (r *Raft) startHeartbeats() {
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			r.mu.Lock()
			if r.state != "Leader" {
				r.mu.Unlock()
				return
			}

			r.mu.Unlock()

			r.sendHeartbeats()
		}
	}()
}

func (r *Raft) InitializeRaft(s *Server) {
	r.server = s
	r.currentTerm = 0
	r.state = "Follower"
	r.votedFor = -1
	r.electionResetTimeout = time.Now()
}
