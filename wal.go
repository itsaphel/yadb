package yadb

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

type walFile struct {
	filename string
}

func (wal *walFile) LoadIntoMap(m map[string]string) {
	f, err := os.OpenFile(wal.filename, os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalln("Failed to open WAL file.", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
		walEntry := &WalEntry{}
		if err := proto.Unmarshal(scanner.Bytes(), walEntry); err != nil {
			log.Fatalln("Encountered error while reading WAL file lines.\nLine:", scanner.Text(), "\nError: ", err)
		}

		if walEntry.Tombstone {
			delete(m, walEntry.Key)
		} else {
			m[walEntry.Key] = walEntry.Value
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalln("Encountered error while reading WAL file.", err)
	}
}

// Write writes information regarding a key-value pair to a log file on disk
// We use Protocol Buffers to serialise the WalEntry into a sequence of bytes
// This log file can be used to recover the in-memory map on restart
//
// Any DML must be logged to the WAL to ensure durability
func (wal *walFile) Write(e *WalEntry) {
	out, err := proto.Marshal(e)
	if err != nil {
		log.Fatalln("Failed to encode WalEntry for persistence.", err)
	}

	f, err := os.OpenFile(wal.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("Failed to open WAL file.", err)
	}
	defer f.Close()

	_, err = f.Write(out)
	if err != nil {
		log.Fatalln("Failed to write WalEntry to disk.", err)
	}
}
