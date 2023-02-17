package main

import (
	"fmt"
	"log"

	bm "adbslab.com/buffer_manager"
	dsm "adbslab.com/data_storage_manager"
)

const (
	DBFFilePath string = "./data.dbf"
)

func ExSetUp() error {
	// Create raw DBF file
	err := bufferManager.Ds.InitFile(DBFFilePath)
	if err != nil {
		log.Printf("Raw DBF file initialization failed: " + err.Error())
	} else {
		log.Printf("Raw DBF file initialization succeeded.")
	}

	// Open raw DBF file
	err = bufferManager.Ds.OpenFile(DBFFilePath)
	if err != nil {
		log.Printf("Open raw DBF file failed: " + err.Error())
	} else {
		log.Printf("Open raw DBF file succeeded.")
	}

	for i := 0; i < 50000; i++ {
		newpage, err := bufferManager.FixNewPage()
		if err != nil {
			log.Printf("Create DBF file failed: Fix new page failed with i = %v.", i)
			return err
		}
		bufferManager.UnfixPage(newpage.Page_id)
	}

	//printStatisticalData()
	resetStatisticalData()

	bufferManager.Ds.CloseFile()
	log.Printf("Create DBF file successful.")
	return nil
}

func resetStatisticalData() {
	bm.Hit = 0
	bm.Miss = 0
	dsm.Read = 0
	dsm.Write = 0
	dsm.IOCounter = 0
}

func printStatisticalData() {
	fmt.Println()

	fmt.Printf("BUFFER STATISTICS:\n")
	fmt.Printf("hit   = %d\n", bm.Hit)
	fmt.Printf("miss  = %d\n", bm.Miss)
	fmt.Printf("rate  = %v\n\n", float64(bm.Hit)/float64(bm.Hit+bm.Miss))

	fmt.Printf("I/O STATISTICS:\n")
	fmt.Printf("total = %d\n", dsm.IOCounter)
	fmt.Printf("read  = %d\n", dsm.Read)
	fmt.Printf("write = %d\n\n", dsm.Write)
}
