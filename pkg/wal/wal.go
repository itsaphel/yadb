package wal

import (
	"io"
	"log"
	"os"

	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"yadb-go/pkg/btree"
	"yadb-go/protoc"
)

// Issues with current WAL approach:
// 1. We can end up with huge WAL files. They're never pruned
// 2. Map always needs to be entirely loaded into memory.
//    So cannot have a Database exceeding memory capacity

type LogFile struct {
	filename string
}

func NewWalFile(filename string) *LogFile {
	return &LogFile{filename: filename}
}

func (logFile *LogFile) ReplayIntoStore(store *btree.Tree) {
	f, err := os.OpenFile(logFile.filename, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalln("Failed to open WAL file.", err)
	}
	defer f.Close()

	for {
		walEntry := &protoc.WalEntry{}
		_, err := pbutil.ReadDelimited(f, walEntry)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln("Encountered error while reading WAL file lines. Error:", err)
			}
		}

		if walEntry.Tombstone {
			store.Delete(walEntry.Key)
		} else {
			store.Insert(walEntry.Key, walEntry.Value)
		}
	}
}

// Write writes information regarding a key-value pair to a log file on disk
// We use Protocol Buffers to serialise the WalEntry into a sequence of bytes
// This log file can be used to recover the in-memory map on restart
//
// Any DML must be logged to the WAL to ensure durability
// TODO should we make every WAL entry one block in size? (i.e. add padding where required)?
// TODO should we Write some kind of checksum (like Luhn's/CRC)? Why?
func (logFile *LogFile) Write(e *protoc.WalEntry) {
	f, err := os.OpenFile(logFile.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("Failed to open WAL file.", err)
	}
	defer f.Close()

	pbutil.WriteDelimited(f, e)
	if err != nil {
		log.Fatalln("Failed to write WalEntry to disk.", err)
	}
	err = f.Sync()
	if err != nil {
		log.Fatalln("Failed to execute fsync WalEntry to disk.", err)
	}
}
