package main

import (
	bm "adbslab.com/buffer_manager"
	dsm "adbslab.com/data_storage_manager"
)

var (
	bufferManager      *bm.BMgr
	dataStorageManager *dsm.DSMgr
)

func main() {

	bufferManager = &bm.BMgr{}
	dataStorageManager = &dsm.DSMgr{}

	err := CreateDBFFile()
	if err != nil {
		panic(err)
	}

	dataStorageManager.OpenFile(DBFFilePath)
}
