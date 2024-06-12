package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex,hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			return fmt.Errorf("error during meta write back, remove db file: %v", e)
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		return fmt.Errorf("error during meta write back , sqlite open : %v", err)
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		return fmt.Errorf("error during meta write back, prepare db : %v", err)
	}
	statement.Exec()
	// panic("todo")
	statement, err = db.Prepare(insertTuple)
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	for _, val := range fileMetas {
		for index, hash_val := range val.BlockHashList {
			statement.Exec(val.Filename, val.Version, index, hash_val)
		}
	}
	return nil

}

/*
Reading Local Metadata File Related
*/
// const getDistinctFileName string = `select distinct fileName from indexes;`

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue from indexes;` //where fileName = ? order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))

	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)

	if e != nil || metaFileStats.IsDir() {
		// if os.IsNotExist(e) {
		// 	fmt.Errorf("index.db doesn't exist: %v", e)
		// 	return fileMetaMap, nil
		// }
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	// panic("todo")
	rows, err := db.Query(getTuplesByFileName)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer rows.Close()

	var fileName string
	var version int
	var hashIndex int
	var hashValue string

	for rows.Next() {
		rows.Scan(&fileName, &version, &hashIndex, &hashValue)
		if _, ok := fileMetaMap[fileName]; !ok { // <nil> false
			//log.Println("Pointer is null")
			hashlist := make([]string, 0)
			hashlist = append(hashlist, hashValue)
			//log.Println(hashlist)
			nc := &FileMetaData{Filename: fileName, Version: int32(version), BlockHashList: hashlist}
			fileMetaMap[fileName] = nc
		} else {
			hashlist := make([]string, 0)
			hashlist = append(hashlist, hashValue)
			//log.Println(hashlist)
			nc := &FileMetaData{Filename: fileName, Version: int32(version), BlockHashList: hashlist}
			fileMetaMap[fileName].Filename = nc.Filename
			fileMetaMap[fileName].Version = nc.Version
			fileMetaMap[fileName].BlockHashList = append(fileMetaMap[fileName].BlockHashList, nc.BlockHashList...)
		}
	}
	//log.Println("Completed Loading Meta File")
	//log.Println(fileMetaMap)
	// db.Close()
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
