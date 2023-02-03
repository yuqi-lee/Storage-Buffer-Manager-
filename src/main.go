package main

import (
	"fmt"
	"log"
	"os"

	bm "adbslab.com/buffer_manager"
	dsm "adbslab.com/data_storage_manager"
)

const (
	BenchPath string = ""
)

var (
	bufferManager      *bm.BMgr
	dataStorageManager *dsm.DSMgr
)

func main() {
	err := CreateDBFFile()
	if err != nil {
		panic(err)
	}

	bufferManager = &bm.BMgr{}
	dataStorageManager = &dsm.DSMgr{}

	bufferManager.Ds.OpenFile(DBFFilePath)

	benchFile, err := os.Open(BenchPath)
	if err != nil {
		panic(err)
	}

	var operation, page, frameID, counter int32
	for {
		n, err := fmt.Fscan(benchFile, "%d,%d", &operation, &page)
		if n == 0 || err != nil {
			break
		}

		counter++
		page -= 1
		frameID, err = bufferManager.FixPage(page, 0)
		if err != nil {
			log.Printf(err.Error())
		} else if operation == 1 {
			//if write, set frame to dirty
			bufferManager.SetDirty(frameID)
		}

		bufferManager.UnfixPage(page)
	}

	bufferManager.WriteDirtys()
	bufferManager.Ds.CloseFile()

	fmt.Printf("total tests: 	%d\n", counter)
	fmt.Printf("hit: 			%d\n", bm.Hit)
	fmt.Printf("miss: 			%d\n", bm.Miss)
	fmt.Printf("read: 			%d\n", dsm.Read)
	fmt.Printf("write: 			%d\n", dsm.Write)
}
