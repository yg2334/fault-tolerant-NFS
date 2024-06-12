package surfstore

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	reflect "reflect"
	"strings"
)

func ClientSync(client RPCClient) {

	local_file_dict := make(map[string][]string)
	new_local_files_wrt_local_idx := make([]string, 0)

	f, err := os.Open(client.BaseDir)
	if err != nil {
		log.Println("error while reading files of base directory")
	}
	fileInfo, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		log.Println("error while reading files of base directory")
	}

	for _, file := range fileInfo {
		if file.Name() == "index.db" {
			continue
		}
		if strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}
		if file.Size() == 0 {
			local_file_dict[file.Name()] = append(local_file_dict[file.Name()], EMPTYFILE_HASHVALUE)
			continue
		}
		fi, err := os.Open(filepath.Join(client.BaseDir, filepath.Base(file.Name())))
		if err != nil {
			log.Printf("file opening error: %v", err)
		}

		for {
			buffer := make([]byte, client.BlockSize)
			n, err := fi.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			// hash := GetBlockHashString(GetBlockHashBytes((buffer[:n])))
			hash := GetBlockHashString(buffer[:n])

			local_file_dict[file.Name()] = append(local_file_dict[file.Name()], hash)
		}

		fi.Close()
	}

	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Println(err)
	}
	for _, file := range fileInfo {
		if file.Name() == "index.db" {
			continue
		}
		if strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}
		_, ok := localIndex[file.Name()]
		if !ok {
			new_local_files_wrt_local_idx = append(new_local_files_wrt_local_idx, file.Name())
		}

	}
	//log.Println("Printing New Files")
	//log.Println(new_local_files_wrt_local_idx)
	remote_fileInfoMap := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remote_fileInfoMap)
	var block_str_addrs []string
	client.GetBlockStoreAddrs(&block_str_addrs)

	var tombstone_hashlist []string
	tombstone_hashlist = append(tombstone_hashlist, TOMBSTONE_HASHVALUE)
	//log.Println(remote_fileInfoMap)
	log.Println(block_str_addrs)
	for key, remote_fileMetaData := range remote_fileInfoMap {
		if key == "index.db" {
			continue
		}
		localfilemetamap, ok := localIndex[key]
		lcl_blocklist, ok1 := local_file_dict[key]
		//log.Println("Printing Boolean")
		//log.Println(ok,ok1)
		if !ok && !ok1 {
			//log.Println("Not present in localindex and base dir")
			//log.Println("1")
			blockhashlist := remote_fileMetaData.BlockHashList
			if !(reflect.DeepEqual(blockhashlist, tombstone_hashlist)) {
				f, err := os.Create(filepath.Join(client.BaseDir, filepath.Base(key)))
				if err != nil {
					log.Println(err)
				}
				f.Close()
				file, _ := os.OpenFile(filepath.Join(client.BaseDir, filepath.Base(key)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
				blockstoremap := make(map[string][]string)
				client.GetBlockStoreMap(blockhashlist, &blockstoremap)
				fmt.Println("BlockStoreMAp: ", blockstoremap)
				addrtoblockMap := make(map[string]string)
				for srvID, hashlist := range blockstoremap {
					for _, hash := range hashlist {
						addrtoblockMap[hash] = srvID
					}
				}
				// for srvID, hashlist := range blockstoremap {
				for _, hash := range blockhashlist {
					var block Block
					if hash == "-1" {
						continue
					}
					//log.Printf("Hash: %s\n",hash)
					e := client.GetBlock(hash, addrtoblockMap[hash], &block)
					if e != nil {
						fmt.Printf("Error In getting block: %v", e)
					}
					//log.Println("Getting Blocks")
					//log.Println(block)
					_, err := file.Write(block.BlockData)
					if err != nil {
						log.Println(err)
					}
				}

				file.Close()
			}
			localIndex[key] = remote_fileMetaData
		} else if ok && !ok1 {
			//log.Println("2")
			if remote_fileMetaData.Version > localfilemetamap.Version {
				//log.Println("21")
				blockhashlist := remote_fileMetaData.BlockHashList
				if !(reflect.DeepEqual(blockhashlist, tombstone_hashlist)) {
					f, err := os.Create(filepath.Join(client.BaseDir, filepath.Base(key)))
					if err != nil {
						fmt.Println(err)
					}
					f.Close()
					file, _ := os.OpenFile(filepath.Join(client.BaseDir, filepath.Base(key)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
					blockstoremap := make(map[string][]string)
					client.GetBlockStoreMap(blockhashlist, &blockstoremap)
					addrtoblockMap := make(map[string]string)
					for srvID, hashlist := range blockstoremap {
						for _, hash := range hashlist {
							addrtoblockMap[hash] = srvID
						}
					}
					// for srvID, hashlist := range blockstoremap {
					for _, hash := range blockhashlist {
						var block Block
						if hash == "-1" {
							continue
						}
						//log.Printf("Hash: %s\n",hash)
						e := client.GetBlock(hash, addrtoblockMap[hash], &block)
						if e != nil {
							fmt.Printf("Error In getting block: %v", e)
						}
						//log.Println("Getting Blocks")
						//log.Println(block)
						_, err := file.Write(block.BlockData)
						if err != nil {
							log.Println(err)
						}
					}

					file.Close()
				}
				localIndex[key] = remote_fileMetaData
			} else if remote_fileMetaData.Version == localfilemetamap.Version {
				if reflect.DeepEqual(remote_fileMetaData.BlockHashList, tombstone_hashlist) {
					continue
				} else {
					//log.Println("22")
					remote_fileMetaData.Version++
					remote_fileMetaData.BlockHashList = tombstone_hashlist
					//localfilemetamap.Version++
					//localfilemetamap.BlockHashList = tombstone_hashlist
					var latestver Version
					e := client.UpdateFile(remote_fileMetaData, &latestver.Version)
					if e != nil {
						log.Println(e)
					}
					localfilemetamap.Version++
					if latestver.Version == localfilemetamap.Version {
						localfilemetamap.BlockHashList = tombstone_hashlist
					}
				}
			}
		} else if !ok && ok1 {
			//log.Println("3")
			blockhashlist := remote_fileMetaData.BlockHashList
			e := os.Remove(filepath.Join(client.BaseDir, filepath.Base(key)))
			if e != nil {
				log.Printf("error during removing file: %v", e)
			}
			if !(reflect.DeepEqual(blockhashlist, tombstone_hashlist)) {
				f, err := os.Create(filepath.Join(client.BaseDir, filepath.Base(key)))
				if err != nil {
					log.Println(err)
				}
				f.Close()
				file, _ := os.OpenFile(filepath.Join(client.BaseDir, filepath.Base(key)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
				blockstoremap := make(map[string][]string)
				client.GetBlockStoreMap(blockhashlist, &blockstoremap)
				addrtoblockMap := make(map[string]string)
				for srvID, hashlist := range blockstoremap {
					for _, hash := range hashlist {
						addrtoblockMap[hash] = srvID
					}
				}
				// for srvID, hashlist := range blockstoremap {
				for _, hash := range blockhashlist {
					var block Block
					if hash == "-1" {
						continue
					}
					//log.Printf("Hash: %s\n",hash)
					e := client.GetBlock(hash, addrtoblockMap[hash], &block)
					if e != nil {
						fmt.Printf("Error In getting block: %v", e)
					}
					//log.Println("Getting Blocks")
					//log.Println(block)
					_, err := file.Write(block.BlockData)
					if err != nil {
						log.Println(err)
					}
				}

				file.Close()
			}
			localIndex[key] = remote_fileMetaData
		} else if ok && ok1 {
			//log.Println("4")
			check := reflect.DeepEqual(lcl_blocklist, localfilemetamap.BlockHashList)
			if check {

				if remote_fileMetaData.Version > localfilemetamap.Version {
					//log.Println("41")
					blockhashlist := remote_fileMetaData.BlockHashList
					e := os.Remove(filepath.Join(client.BaseDir, filepath.Base(key)))
					if e != nil {
						log.Println("error during removing file")
					}
					if !(reflect.DeepEqual(blockhashlist, tombstone_hashlist)) {
						f, err := os.Create(filepath.Join(client.BaseDir, filepath.Base(key)))
						if err != nil {
							log.Println(err)
						}
						f.Close()
						file, _ := os.OpenFile(filepath.Join(client.BaseDir, filepath.Base(key)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
						blockstoremap := make(map[string][]string)
						client.GetBlockStoreMap(blockhashlist, &blockstoremap)
						addrtoblockMap := make(map[string]string)
						for srvID, hashlist := range blockstoremap {
							for _, hash := range hashlist {
								addrtoblockMap[hash] = srvID
							}
						}
						// for srvID, hashlist := range blockstoremap {
						for _, hash := range blockhashlist {
							var block Block
							if hash == "-1" {
								continue
							}
							//log.Printf("Hash: %s\n",hash)
							e := client.GetBlock(hash, addrtoblockMap[hash], &block)
							if e != nil {
								fmt.Printf("Error In getting block: %v", e)
							}
							//log.Println("Getting Blocks")
							//log.Println(block)
							_, err := file.Write(block.BlockData)
							if err != nil {
								log.Println(err)
							}
						}

						file.Close()
					}
					localIndex[key] = remote_fileMetaData
				}
			} else {
				if remote_fileMetaData.Version > localfilemetamap.Version {
					blockhashlist := remote_fileMetaData.BlockHashList
					e := os.Remove(filepath.Join(client.BaseDir, filepath.Base(key)))
					if e != nil {
						log.Println("error during removing file")
					}
					if !(reflect.DeepEqual(blockhashlist, tombstone_hashlist)) {
						f, err := os.Create(filepath.Join(client.BaseDir, filepath.Base(key)))
						if err != nil {
							log.Println(err)
						}
						f.Close()
						file, _ := os.OpenFile(filepath.Join(client.BaseDir, filepath.Base(key)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
						blockstoremap := make(map[string][]string)
						client.GetBlockStoreMap(blockhashlist, &blockstoremap)
						addrtoblockMap := make(map[string]string)
						for srvID, hashlist := range blockstoremap {
							for _, hash := range hashlist {
								addrtoblockMap[hash] = srvID
							}
						}
						// for srvID, hashlist := range blockstoremap {
						for _, hash := range blockhashlist {
							var block Block
							if hash == "-1" {
								continue
							}
							//log.Printf("Hash: %s\n",hash)
							e := client.GetBlock(hash, addrtoblockMap[hash], &block)
							if e != nil {
								fmt.Printf("Error In getting block: %v", e)
							}
							//log.Println("Getting Blocks")
							//log.Println(block)
							_, err := file.Write(block.BlockData)
							if err != nil {
								log.Println(err)
							}
						}

						file.Close()
					}
					localIndex[key] = remote_fileMetaData
				} else if remote_fileMetaData.Version == localfilemetamap.Version {
					//log.Println("44")
					//log.Println(lcl_blocklist)
					remote_fileMetaData.Version++
					remote_fileMetaData.BlockHashList = lcl_blocklist
					fi, err := os.Open(filepath.Join(client.BaseDir, filepath.Base(key)))
					if err != nil {
						log.Printf("file opening error: %v", err)
					}
					blockstoremap1 := make(map[string][]string)
					client.GetBlockStoreMap(lcl_blocklist, &blockstoremap1)
					fmt.Println("Block Store Map: ", blockstoremap1)
					for {
						buffer := make([]byte, client.BlockSize)
						n, err := fi.Read(buffer)
						if err == io.EOF {
							break
						}
						if err != nil {
							break
						}
						var block Block
						var succ bool
						block.BlockData = buffer[:n]
						block.BlockSize = int32(n)
						blockstoremap := make(map[string][]string)
						hashes := make([]string, 0)
						// hashes = append(hashes, GetBlockHashString(GetBlockHashBytes(block.BlockData)))
						hashes = append(hashes, GetBlockHashString(block.BlockData))
						client.GetBlockStoreMap(hashes, &blockstoremap)
						for srv_ID, hashlist := range blockstoremap {
							for _, hash := range hashlist {
								e := client.PutBlock(&block, srv_ID, &succ)
								if e != nil {
									log.Println(hash)
									fmt.Println(e)
								}
							}
						}
					}
					var latestver Version
					e := client.UpdateFile(remote_fileMetaData, &latestver.Version)
					if e != nil {
						log.Println(e)
					}
					localfilemetamap.Version++
					if latestver.Version == localfilemetamap.Version {
						localfilemetamap.BlockHashList = lcl_blocklist
					}
				}
			}
		}
	}

	for _, file := range new_local_files_wrt_local_idx {
		//log.Println("parsing new files")
		_, ok := remote_fileInfoMap[file]
		if !ok {
			//log.Println("Uploading to Remote")
			//var filemetamap FileMetaData
			nc := FileMetaData{Filename: file, Version: 1, BlockHashList: local_file_dict[file]}
			//filemetamap.Filename = file
			//filemetamap.Version = 1
			//filemetamap.BlockHashList = local_file_dict[file]
			//remote_fileMetaData := nc
			blockstoremap1 := make(map[string][]string)
			client.GetBlockStoreMap(local_file_dict[file], &blockstoremap1)
			fmt.Println("Block Store Map: ", blockstoremap1)
			fi, err := os.Open(filepath.Join(client.BaseDir, filepath.Base(file)))
			if err != nil {
				log.Printf("file opening error: %v", err)
			}
			for {
				buffer := make([]byte, client.BlockSize)
				n, err := fi.Read(buffer)
				if err == io.EOF {
					break
				}
				if err != nil {
					break
				}
				var block Block
				var succ bool
				block.BlockData = buffer[:n]
				block.BlockSize = int32(n)
				blockstoremap := make(map[string][]string)
				hashes := make([]string, 0)
				// hashes = append(hashes, GetBlockHashString(GetBlockHashBytes(block.BlockData)))
				hashes = append(hashes, GetBlockHashString(block.BlockData))
				client.GetBlockStoreMap(hashes, &blockstoremap)
				for srv_ID, hashlist := range blockstoremap {
					for _, hash := range hashlist {
						e := client.PutBlock(&block, srv_ID, &succ)
						if e != nil {
							log.Println(hash)
							fmt.Println(e)
						}
					}
				}
			}

			var latestver Version
			e := client.UpdateFile(&nc, &latestver.Version)
			if e != nil {
				log.Println(e)
			}
			// localIndex[file].Version = 1
			if latestver.Version == 1 {
				localIndex[file] = &nc
			}

		}
	}
	//log.Println("DOne")
	//log.Println(localIndex)
	e := WriteMetaFile(localIndex, client.BaseDir)
	if e != nil {
		log.Println(e)
	}

}
