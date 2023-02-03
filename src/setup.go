package main

import (
	"log"

	bm "adbslab.com/buffer_manager"
)

const (
	DBFFilePath string = "./data.dbf"
)

func CreateDBFFile() error {
	bufferManager = &bm.BMgr{}
	bufferManager.Ds.InitFile(DBFFilePath)
	bufferManager.Ds.OpenFile(DBFFilePath)
	for i := 0; i < 50000; i++ {
		newpage, err := bufferManager.FixNewPage()
		if err != nil {
			log.Printf("Create dbf-file failed: Fix new page failed.")
			return err
		}
		bufferManager.UnfixPage(newpage.Page_id)
	}

	bufferManager.Ds.CloseFile()
	log.Printf("Create dbf-file success.")
	return nil
}
