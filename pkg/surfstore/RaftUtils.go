package surfstore

import (
	"bufio"
	context "context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),

		id:          id,
		commitIndex: -1,

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,

		//New Additions
		peers:           config.RaftAddrs,
		pendingRequests: make([]*chan PendingRequest, 0),
		lastApplied:     -1,
		updateStatus:    PeerUpdateStatus_PeerUnknown,
		nextIndex:       make([]int64, len(config.RaftAddrs)),
		matchIndex:      make([]int64, len(config.RaftAddrs)),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	// panic("todo")
	RegisterRaftSurfstoreServer(server.grpcServer, server)

	log.Println("Started Raft Server with id: ", server.id)

	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}

	return server.grpcServer.Serve(l)

}

func (s *RaftSurfstore) checkStatus() error {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if serverStatus != ServerStatus_LEADER {
		return ErrNotLeader
	}

	return nil
}

func (s *RaftSurfstore) sendPersistentHeartbeats(ctx context.Context, reqId int64) (*Success, error) {
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	flag := false
	s.raftStateMutex.RLock()
	numServers := len(s.peers)
	peers := s.peers
	newentryIdx := int64(len(s.log) - 1)
	prevlogindex := int64(0)
	prevlogterm := int64(0)
	if newentryIdx != 0 {
		prevlogindex = newentryIdx - 1
		prevlogterm = s.log[newentryIdx-1].Term
	}
	s.raftStateMutex.RUnlock()

	peerResponses := make(chan bool, numServers-1)

	for idx := range peers {
		s.raftStateMutex.RLock()
		entriesToSend := s.log[:newentryIdx+1]
		s.raftStateMutex.RUnlock()

		idx := int64(idx)

		if idx == s.id {
			continue
		}

		//TODO: Utilize next index

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

func (s *RaftSurfstore) sendToFollower(ctx context.Context, peerId int64, entries []*UpdateOperation, peerResponses chan<- bool, prevlogindex int64, prevlogterm int64) {

	client := NewRaftSurfstoreClient(s.rpcConns[peerId])

	s.raftStateMutex.RLock()
	appendEntriesInput := AppendEntryInput{
		Term:         s.term,
		LeaderId:     s.id,
		PrevLogTerm:  prevlogterm,
		PrevLogIndex: prevlogindex,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	s.raftStateMutex.RUnlock()

	reply, err := client.AppendEntries(ctx, &appendEntriesInput)
	if err != nil {
		time.Sleep(100 * time.Millisecond)
		peerResponses <- false
	} else if reply != nil && reply.Success {
		s.raftStateMutex.Lock()
		s.nextIndex[reply.ServerId] = reply.MatchedIndex + int64(1)
		s.matchIndex[reply.ServerId] = reply.MatchedIndex
		s.raftStateMutex.Unlock()
		peerResponses <- true
		fmt.Println("Server", s.id, ": Receiving output:", "Term", reply.Term, "Id", reply.ServerId, "Success", reply.Success, "Matched Index", reply.MatchedIndex)
	} else if s.term < reply.Term {
		peerResponses <- false
	} else {
		s.raftStateMutex.Lock()
		s.nextIndex[reply.ServerId] -= int64(1)
		s.matchIndex[reply.ServerId] -= int64(1)
		s.raftStateMutex.Unlock()
		peerResponses <- false
	}

}
