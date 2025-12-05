package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	// in our case we simply pass the variable value
	command int
	// term of the entry
	term int
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

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	fmt.Printf("Received append entries from %d\n", args.LeaderId)
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		fmt.Printf("\tReturned false due to term")
		return nil
	}

	if args.Term == r.currentTerm {
		r.mu.Lock()
		r.electionResetTimeout = time.Now()
		r.mu.Unlock()
	}

	return nil
}

// Function that runs the election timer
func (r *Raft) runElectionTimer() {
	timeout := getElectionTimeout()
	r.mu.Lock()
	timerStartTerm := r.currentTerm
	r.mu.Unlock()
	fmt.Printf("Election Timer started with %v, term=%d", timeout, timerStartTerm)

	// loop infinitely, check every 10ms for whether any change has occured
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		r.mu.Lock()
		// end the timer if we are leader
		if r.state != "Candidate" && r.state != "Follower" {
			fmt.Printf("Election Timer ending as server is a leader\n")
			r.mu.Unlock()
			return
		}

		// end the timer if the term has changed.
		if timerStartTerm != r.currentTerm {
			fmt.Printf("Election timer ending as term not same")
			r.mu.Unlock()
			return
		}

		// if the time elapsed since hte last election timeout is done then become a candidate
		if elapsed := time.Since(r.electionResetTimeout); elapsed >= timeout {
			r.startElection()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

// Gets a random eletion timeout
func getElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (r *Raft) startElection() {
	fmt.Printf("Starting election")
}
