package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	// return *FileInfoMap
	log.Println("File meta Map")
	//log.Println(m.FileMetaMap)
	filemap := m.FileMetaMap
	return &FileInfoMap{FileInfoMap: filemap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	fmt.Println("Update Map")

	filedata, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		//log.Println(m.FileMetaMap[fileMetaData.Filename])
		return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, nil
	} else {
		if filedata.Version+1 == (fileMetaData.Version) {
			m.FileMetaMap[fileMetaData.Filename].Version = fileMetaData.Version
			m.FileMetaMap[fileMetaData.Filename].BlockHashList = fileMetaData.BlockHashList
			//log.Println(m.FileMetaMap[fileMetaData.Filename])
			return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, nil
		} else {
			return &Version{Version: -1}, fmt.Errorf("file version %v is not exactly one greater", fileMetaData.Version)
		}
	}

}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	blockstoremap := make(map[string][]string, 0)
	hashesIn := blockHashesIn.Hashes
	for _, hash := range hashesIn {
		srvID := m.ConsistentHashRing.GetResponsibleServer(hash)
		blockstoremap[srvID] = append(blockstoremap[srvID], hash)
	}
	blockstore := make(map[string]*BlockHashes, 0)
	for k, v := range blockstoremap {
		nc := &BlockHashes{Hashes: v}
		blockstore[k] = nc
	}

	return &BlockStoreMap{BlockStoreMap: blockstore}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
