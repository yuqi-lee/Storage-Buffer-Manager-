package main

import (
	"log"
)

const (
	DBFFilePath string = "./data.dbf"
)

func CreateDBFFile() error {
	err := bufferManager.Ds.InitFile(DBFFilePath)
	if err != nil {
		log.Printf("DBF initialization failed: " + err.Error())
	} else {
		log.Printf("DBF initialization succeeded.")
	}
	bufferManager.Ds.OpenFile(DBFFilePath)
	for i := 0; i < 50000; i++ {
		newpage, err := bufferManager.FixNewPage()
		if err != nil {
			log.Printf("Create dbf-file failed: Fix new page failed with i = %v.", i)
			return err
		}
		bufferManager.UnfixPage(newpage.Page_id)
	}

	bufferManager.Ds.CloseFile()
	log.Printf("Create dbf-file success.")
	return nil
}
