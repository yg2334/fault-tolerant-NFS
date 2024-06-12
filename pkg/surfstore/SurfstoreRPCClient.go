package surfstore

import (
	context "context"
	"fmt"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// panic("todo")
	// log.Println("Getting Block Hashes")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bh, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	//log.Println(b)

	*blockHashes = bh.Hashes
	//log.Println(block)
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	fmt.Println("Getting Blocks From Server")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	//log.Println(b)

	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	//log.Println(block)
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	// connect to the server
	log.Println("Putting Blocks to Server")
	//log.Println(block)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// Here this function comes from generate code from proto
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, &Block{BlockData: block.BlockData, BlockSize: block.BlockSize})
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.Flag
	// close the connection
	return conn.Close()

}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// Here this function comes from generate code from proto
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hl, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = hl.Hashes
	// close the connection
	return conn.Close()

}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")
	// connect to the server
	for _, server := range surfClient.MetaStoreAddrs {
		//log.Println("Getting Server FileInfo")

		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		infomap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			// panic("todo")
			if err == ErrNotLeader {
				log.Println(err)
				continue
			} else if err == ErrServerCrashed || err == ErrServerCrashedUnreachable {
				log.Println(err)
				continue
			}
			conn.Close()
			return err
		}
		*serverFileInfoMap = infomap.FileInfoMap
		//log.Println(*serverFileInfoMap)
		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")
	// connect to the server
	//log.Println("Updating File Server")
	//log.Println(fileMetaData)
	for _, server := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ver, err := c.UpdateFile(ctx, &FileMetaData{Filename: fileMetaData.Filename, Version: fileMetaData.Version, BlockHashList: fileMetaData.BlockHashList})
		if err != nil {
			// panic("todo")
			if err == ErrNotLeader {
				log.Println(err)
				continue
			} else if err == ErrServerCrashed || err == ErrServerCrashedUnreachable {
				log.Println(err)
				continue
			}
			conn.Close()
			return err
		}
		*latestVersion = ver.Version

		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")

}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// panic("todo")
	for _, server := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bstrmap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			// panic("todo")
			if err == ErrNotLeader {
				log.Println(err)
				continue
			} else if err == ErrServerCrashed || err == ErrServerCrashedUnreachable {
				log.Println(err)
				continue
			}
			conn.Close()
			return err
		}
		blmap := make(map[string][]string, 0)
		for k, v := range bstrmap.BlockStoreMap {
			blmap[k] = v.Hashes
		}
		*blockStoreMap = blmap
		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// panic("todo")
	for _, server := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blstraddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			// panic("todo")
			if err == ErrNotLeader {
				log.Println(err)
				continue
			} else if err == ErrServerCrashed || err == ErrServerCrashedUnreachable {
				log.Println(err)
				continue
			}
			conn.Close()
			return err
		}
		*blockStoreAddrs = blstraddrs.BlockStoreAddrs

		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")

}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
