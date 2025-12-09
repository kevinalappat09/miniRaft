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

	// 1. Reply false if leader's term < currentTerm
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return nil
	}

	// 2. If leader's term > currentTerm, update term and become follower
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = -1
		r.state = "Follower"
	}

	// 3. Reset election timer
	r.electionResetTimeout = time.Now()

	// 4. Log consistency check
	prevIndex := args.PreviousLogIndex
	prevTerm := args.PreviousLogTerm

	if prevIndex >= 0 {
		if prevIndex >= len(r.log) || r.log[prevIndex].Term != prevTerm {
			// Follower log doesn't match leader
			reply.Term = r.currentTerm
			reply.Success = false
			return nil
		}
	}

	// 5. Append the new entry only if it’s not a heartbeat (empty entry)
	if args.Entry != (LogEntry{}) {
		if prevIndex+1 < len(r.log) {
			// Conflict: truncate follower log
			r.log = r.log[:prevIndex+1]
		}
		r.log = append(r.log, args.Entry)
	}

	// 6. Update commit index
	if args.LeaderCommitIndex > r.commitIndex {
		newCommit := args.LeaderCommitIndex
		if newCommit > len(r.log)-1 {
			newCommit = len(r.log) - 1
		}
		r.commit(newCommit)
	}

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

	fmt.Printf("[server %d] RequestVotes received from server : %d\n", r.server.id, args.Id)
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		fmt.Printf("[server %d] Rejected RequestVotes as current term higher\n", r.server.id)
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
		fmt.Printf("[server %d] Rejected vote: log not up-to-date", r.server.id)
		return nil
	}

	if r.votedFor == -1 || r.votedFor == args.Id {
		r.votedFor = args.Id
		r.electionResetTimeout = time.Now()

		reply.Term = r.currentTerm
		reply.VoteGranted = true
		fmt.Printf("[server %d] Accepted vote", r.server.id)
		return nil
	}
	reply.Term = r.currentTerm
	reply.VoteGranted = false
	fmt.Printf("[server %d] Rejected vote: already voted", r.server.id)

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
		r.becomeLeader(lastIndex, term)
		r.mu.Unlock()
		return
	}

	r.becomeFollower()
	r.mu.Unlock()
}

func (r *Raft) becomeLeader(lastIndex, term int) {
	r.state = "Leader"
	for i, _ := range r.server.peers {
		r.nextIndex[i] = lastIndex + 1
		r.matchIndex[i] = 0
	}
	fmt.Printf("[server %d] BECAME LEADER for term %d\n", r.server.id, term)
	r.startHeartbeats()
}

func (r *Raft) becomeFollower() {
	r.votedFor = -1
	r.electionResetTimeout = time.Now()
	r.state = "Follower"
	fmt.Printf("[server %d] Becoming Follower\n", r.server.id)
}

func (r *Raft) sendHeartbeats() {
	r.mu.Lock()
	if r.state != "Leader" {
		r.mu.Unlock()
		return
	}

	term := r.currentTerm
	commitIndex := r.commitIndex
	r.mu.Unlock()

	for idx, peer := range r.server.peers {
		go func(peer string, idx int) {
			r.mu.Lock()
			prevIndex := r.nextIndex[idx] - 1
			prevTerm := 0
			if prevIndex >= 0 && prevIndex < len(r.log) {
				prevTerm = r.log[prevIndex].Term
			}
			r.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:              term,
				LeaderId:          r.server.id,
				PreviousLogIndex:  prevIndex,
				PreviousLogTerm:   prevTerm,
				Entry:             LogEntry{}, // empty = heartbeat
				LeaderCommitIndex: commitIndex,
			}

			_, _ = r.callAppendEntries(peer, *args)
		}(peer, idx)
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

func (r *Raft) commit(commitIndex int) {
	fmt.Printf("[server %d] commit() called with commitIndex=%d (current commitIndex=%d lastApplied=%d)\n",
		r.server.id, commitIndex, r.commitIndex, r.lastApplied)

	// do not commit entries already committed
	if commitIndex <= r.commitIndex {
		fmt.Printf("[server %d] Already committed up to %d, nothing to do\n", r.server.id, r.commitIndex)
		return
	}

	// clamp to last valid log index
	if commitIndex > len(r.log)-1 {
		fmt.Printf("[server %d] commitIndex %d > maxLogIndex %d, adjusting\n",
			r.server.id, commitIndex, len(r.log)-1)
		commitIndex = len(r.log) - 1
	}

	fmt.Printf("[server %d] Applying log entries from %d to %d\n",
		r.server.id, r.lastApplied+1, commitIndex)

	// apply all entries including commitIndex
	for i := r.lastApplied + 1; i <= commitIndex; i++ {
		entry := r.log[i]
		r.server.value = entry.Command
		fmt.Printf("[server %d] **APPLIED** log[%d] = %d (term %d)\n",
			r.server.id, i, entry.Command, entry.Term)
		r.lastApplied = i
	}

	r.commitIndex = commitIndex
	fmt.Printf("[server %d] commitIndex updated to %d\n", r.server.id, r.commitIndex)
}

func (r *Raft) handleCommand(command int) bool {
	fmt.Printf("[server %d] handleCommand called with value %d\n", r.server.id, command)

	r.mu.Lock()
	// Step 1: append new log entry
	logEntry := LogEntry{
		Term:    r.currentTerm,
		Command: command,
	}
	r.log = append(r.log, logEntry)
	index := len(r.log) - 1
	term := r.currentTerm
	fmt.Printf("[server %d] Appended log entry at index %d with term %d\n", r.server.id, index, term)
	r.mu.Unlock()

	var wg sync.WaitGroup

	for idx, peer := range r.server.peers {
		wg.Add(1)
		go func(peer string, idx int) {
			defer wg.Done()
			for {
				r.mu.Lock()
				nextIdx := r.nextIndex[idx]
				prevLogIndex := nextIdx - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(r.log) {
					prevLogTerm = r.log[prevLogIndex].Term
				}
				entries := r.log[nextIdx:]
				r.mu.Unlock()

				fmt.Printf("[server %d] Sending AppendEntries to peer %s: prevLogIndex=%d, prevLogTerm=%d, entriesLen=%d\n",
					r.server.id, peer, prevLogIndex, prevLogTerm, len(entries))

				args := &AppendEntriesArgs{
					Term:              r.currentTerm,
					LeaderId:          r.server.id,
					PreviousLogIndex:  prevLogIndex,
					PreviousLogTerm:   prevLogTerm,
					Entry:             logEntry,
					LeaderCommitIndex: r.commitIndex,
				}

				success, reply := r.callAppendEntries(peer, *args)
				if !success {
					fmt.Printf("[server %d] AppendEntries RPC failed to %s\n", r.server.id, peer)
					time.Sleep(50 * time.Millisecond)
					continue
				}

				r.mu.Lock()
				if reply.Term > term {
					fmt.Printf("[server %d] Peer %s has higher term %d, stepping down from term %d\n",
						r.server.id, peer, reply.Term, term)
					r.currentTerm = reply.Term
					r.becomeFollower()
					r.mu.Unlock()
					return
				}

				if reply.Success {
					r.nextIndex[idx] = nextIdx + len(entries)
					r.matchIndex[idx] = r.nextIndex[idx] - 1
					fmt.Printf("[server %d] AppendEntries to %s succeeded: nextIndex=%d, matchIndex=%d\n",
						r.server.id, peer, r.nextIndex[idx], r.matchIndex[idx])
					r.mu.Unlock()
					break
				} else {
					fmt.Printf("[server %d] AppendEntries to %s failed due to log inconsistency, decrementing nextIndex from %d\n",
						r.server.id, peer, r.nextIndex[idx])
					if r.nextIndex[idx] > 0 {
						r.nextIndex[idx]--
					}
					r.mu.Unlock()
				}
			}
		}(peer, idx)
	}

	wg.Wait()

	r.mu.Lock()
	committed := false
	count := 1 // leader itself
	for _, match := range r.matchIndex {
		if match >= index {
			count++
		}
	}
	majority := len(r.server.peers)/2 + 1
	fmt.Printf("[server %d] Checking if log at index %d can be committed: matchCount=%d, majority=%d\n",
		r.server.id, index, count, majority)

	if count >= majority && r.log[index].Term == r.currentTerm {
		r.commit(index)
		fmt.Printf("[server %d] Log at index %d committed, value=%d\n", r.server.id, index, r.log[index].Command)
		committed = true
	} else {
		fmt.Printf("[server %d] Log at index %d not committed yet\n", r.server.id, index)
	}
	r.mu.Unlock()

	return committed
}

func (r *Raft) InitializeRaft(s *Server) {
	r.server = s
	r.currentTerm = 0
	r.state = "Follower"
	r.votedFor = -1
	r.commitIndex = -1
	r.lastApplied = -1
	r.electionResetTimeout = time.Now()

	r.nextIndex = make([]int, len(r.server.peers))
	r.matchIndex = make([]int, len(r.server.peers))
}
