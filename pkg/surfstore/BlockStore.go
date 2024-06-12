package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	log.Println("Get BlocksA")
	//log.Println(blockHash.Hash)
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		//log.Println("Get Blocks1")
		blockdata := make([]byte, 0)
		return &Block{BlockData: blockdata, BlockSize: 0}, fmt.Errorf("Block with hash %v not present in block store", blockHash.Hash)
	} else {
		//log.Println(bs.BlockMap[blockHash.Hash])
		//log.Println(block.BlockData)
		//log.Println("Get Blocks")
		return &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	log.Println("Putting Blocks")
	//log.Println(block.BlockData)
	// hashBytes := GetBlockHashBytes(block.BlockData) //sha256.Sum256(block.BlockData)
	// hashString := GetBlockHashString(hashBytes)     //hex.EncodeToString(hashBytes[:])
	hashString := GetBlockHashString(block.BlockData) //hex.EncodeToString(hashBytes[:])

	// if !ok{
	// 	return &Success{Flag: false}, fmt.Errorf("Error in hashing block ")
	// }

	bs.BlockMap[hashString] = block

	//log.Println(bs.BlockMap[hashString])
	//log.Printf("Hash String : %s\n",hashString)
	return &Success{Flag: true}, nil

}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	blockHashesOut := make([]string, 0)
	for _, hash := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[hash]
		if !ok {
			blockHashesOut = append(blockHashesOut, hash)
		}
	}
	return &BlockHashes{Hashes: blockHashesOut}, nil

}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	hashes := make([]string, 0)
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
