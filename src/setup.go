package main

import (
	"log"
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

	bufferManager.Ds.CloseFile()
	log.Printf("Create DBF file successful.")
	return nil
}
