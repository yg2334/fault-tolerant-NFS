package surfstore

import (
	context "context"
	"log"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	//New Additions
	peers           []string
	pendingRequests []*chan PendingRequest
	lastApplied     int64
	updateStatus    PeerUpdateStatus
	nextIndex       []int64
	matchIndex      []int64
	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	for {
		check, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if check.Flag {
			break
		}
	}

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	for {
		check, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if check.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	for {
		check, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if check.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	// pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &entry)

	// s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request
	// reqId := len(s.pendingRequests) - 1
	numServers := len(s.peers)
	s.raftStateMutex.Unlock()

	// go s.sendPersistentHeartbeats(ctx, int64(reqId))
	for {
		check, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if check.Flag {
			//TODO:
			// Ensure that leader commits first and then applies to the state machine
			s.raftStateMutex.Lock()
			// s.commitIndex += 1
			N := int64(len(s.log) - 1)
			for N > s.commitIndex {
				majority := 1
				if s.log[N].Term == s.term {
					for _, i := range s.matchIndex {
						if int64(i) >= N {
							majority++
						}
					}
					if majority > numServers/2 {
						s.commitIndex = N
						for s.lastApplied < s.commitIndex-1 {
							entry1 := s.log[s.lastApplied+1]
							if entry1.FileMetaData != nil {
								_, err := s.metaStore.UpdateFile(ctx, entry1.FileMetaData)
								if err != nil {
									s.raftStateMutex.Unlock()
									return nil, err
								}
							}
							s.lastApplied += 1
						}
						s.lastApplied = s.commitIndex
						if entry.FileMetaData != nil {
							ver, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
							if err != nil {
								s.raftStateMutex.Unlock()
								return nil, err
							} else {
								s.raftStateMutex.Unlock()
								return ver, err
							}
						}
					}
				}
				N--
			}
			s.raftStateMutex.Unlock()
		}
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	s.raftStateMutex.RLock()
	unrchble := s.unreachableFrom[input.LeaderId]
	s.raftStateMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED || unrchble {
		log.Println("server crashed")
		return nil, ErrServerCrashedUnreachable
	}
	//check the status
	s.raftStateMutex.RLock()
	peerTerm := s.term
	peerId := s.id
	logLength := int64(len(s.log))
	s.raftStateMutex.RUnlock()

	if peerTerm > input.Term {
		return &AppendEntryOutput{
			Term:         peerTerm,
			ServerId:     peerId,
			Success:      false,
			MatchedIndex: int64(-1),
		}, nil
	}

	if peerTerm < input.Term {
		s.serverStatusMutex.Lock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.Unlock()

		s.raftStateMutex.Lock()
		s.term = input.Term
		s.raftStateMutex.Unlock()

		peerTerm = input.Term
	}

	if input.PrevLogIndex != -1 {
		if logLength > input.PrevLogIndex {
			s.raftStateMutex.RLock()
			peerPrevLogTerm := s.log[input.PrevLogIndex].Term
			s.raftStateMutex.RUnlock()
			if peerPrevLogTerm != input.PrevLogTerm {
				return &AppendEntryOutput{
					Term:         peerTerm,
					ServerId:     peerId,
					Success:      false,
					MatchedIndex: int64(-1),
				}, nil
			}
		} else {
			return &AppendEntryOutput{
				Term:         peerTerm,
				ServerId:     peerId,
				Success:      false,
				MatchedIndex: int64(-1),
			}, nil
		}
	}

	s.raftStateMutex.Lock()
	if logLength > input.PrevLogIndex+1 {
		// if s.log[input.PrevLogIndex+1].Term != input.Entries[input.PrevLogIndex+1].Term {
		s.log = s.log[:input.PrevLogIndex+1]
		// }
	}
	s.raftStateMutex.Unlock()

	s.raftStateMutex.Lock()
	s.log = append(s.log, input.Entries...)
	log.Println("ServerId: ", s.id, " Entries: ", s.log)
	s.raftStateMutex.Unlock()

	//TODO: Change per algorithm
	dummyAppendEntriesOutput := AppendEntryOutput{
		Term:         peerTerm,
		ServerId:     peerId,
		Success:      false,
		MatchedIndex: int64(-1),
	}

	//TODO: Change this per algorithm
	s.raftStateMutex.Lock()
	if s.commitIndex < input.LeaderCommit {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))

		for s.lastApplied < s.commitIndex {
			entry := s.log[s.lastApplied+1]
			if entry.FileMetaData != nil {
				_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
				if err != nil {
					s.raftStateMutex.Unlock()
					return nil, err
				}
			}
			s.lastApplied += 1
		}
	}
	dummyAppendEntriesOutput.MatchedIndex = int64(len(s.log) - 1)
	dummyAppendEntriesOutput.Success = true
	log.Println("Server", s.id, ": Sending output:", "Term", dummyAppendEntriesOutput.Term, "Id", dummyAppendEntriesOutput.ServerId, "Success", dummyAppendEntriesOutput.Success, "Matched Index", dummyAppendEntriesOutput.MatchedIndex)
	s.raftStateMutex.Unlock()

	return &dummyAppendEntriesOutput, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.term += 1
	s.raftStateMutex.Unlock()

	//TODO: update the state
	// pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	for idx := range s.peers {
		idx := int64(idx)
		s.matchIndex[idx] = int64(0)
		s.nextIndex[idx] = int64(len(s.log))
	}
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: nil,
	}
	s.log = append(s.log, &entry)
	log.Println("Leader Logs: ", s.log)
	// s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request
	// reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	for {
		// check, err := s.sendPersistentHeartbeats(ctx, int64(reqId))
		check, err := s.SendHeartbeat(ctx, &emptypb.Empty{})

		if err != nil {
			return nil, err
		}
		if check.Flag {
			log.Println("Leader Appended No-op entry")
			break
		}
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	flag := false
	prevlogindex := int64(-1)
	prevlogterm := int64(0)
	s.raftStateMutex.RLock()
	serverID := s.id
	numServers := len(s.peers)
	peers := s.peers
	s.raftStateMutex.RUnlock()

	peerResponses := make(chan bool, numServers-1)

	for idx := range peers {
		idx := int64(idx)

		if idx == serverID {
			continue
		}

		//TODO: Utilize next index
		s.raftStateMutex.RLock()
		if s.nextIndex[idx] >= 1 {
			prevlogindex = s.nextIndex[idx] - 1
			prevlogterm = s.log[s.nextIndex[idx]-1].Term
		}
		entriesToSend := s.log[s.nextIndex[idx]:]
		log.Println("Leader id: ", s.id, " EntriestoSend: ", entriesToSend, " to ServerId: ", idx)
		s.raftStateMutex.RUnlock()

		go s.sendToFollower(ctx, idx, entriesToSend, peerResponses, prevlogindex, prevlogterm)
	}

	totalResponses := 1
	numAliveServers := 1
	for totalResponses < numServers {
		response := <-peerResponses
		totalResponses += 1
		if response {
			numAliveServers += 1
		}
	}

	if numAliveServers > numServers/2 {
		flag = true
	}
	// }
	return &Success{Flag: flag}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	if len(servers.ServerIds) == 0 {
		s.unreachableFrom = make(map[int64]bool)
		log.Printf("Server %d is reachable from all servers", s.id)
	} else {
		for _, serverId := range servers.ServerIds {
			s.unreachableFrom[serverId] = true
		}
		log.Printf("Server %d is unreachable from %v", s.id, s.unreachableFrom)
	}

	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
