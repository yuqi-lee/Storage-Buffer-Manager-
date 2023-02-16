package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	bm "adbslab.com/buffer_manager"
	dsm "adbslab.com/data_storage_manager"
)

const (
	BenchPath string = "./data-5w-50w-zipf.txt"
)

var (
	bufferManager *bm.BMgr = &bm.BMgr{}
)

func main() {
	bufferManager.Init()

	err := ExSetUp()
	if err != nil {
		panic(err)
	}

	//bufferManager.Lru = bm.New(int64(bm.BufferSize*4), nil)
	//log.Printf("Current size of buffer: 		%v", bufferManager.Lru.Size())
	//log.Printf("Max size of buffer:		 	%v", bufferManager.Lru.Cap())

	f, err := os.OpenFile(BenchPath, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := bufio.NewReader(f)

	var operation, page, frameID, counter int32
	t1 := time.Now()
	for {
		rawStr, err := buf.ReadString('\n')
		if err != nil {
			log.Printf(err.Error())
			break
		}
		// log.Printf("raw strings: %v", rawStr)

		strs := strings.Split(rawStr, ",")
		// log.Printf("strings: %s : %s", strs[0], strs[1])
		if len(strs) != 2 {
			log.Printf("Unexcepted error in bench file.")
			continue
		} else {
			operationInt, _ := strconv.Atoi(strs[0])
			pageInt, err := strconv.Atoi(strs[1][0 : len(strs[1])-2])
			if err != nil {
				log.Printf(err.Error())
			}
			operation = int32(operationInt)
			page = int32(pageInt)
		}
		// log.Printf("operation: %v, page: %v", operation, page)

		counter++
		/*
			debug
				if counter > 10 { // debug
					break
				}
		*/
		page -= 1
		frameID, err = bufferManager.FixPage(page, 0)
		if err != nil {
			log.Printf(err.Error())
		} else if operation == 1 { // operation WRITE
			bufferManager.SetDirty(frameID)
		}

		bufferManager.UnfixPage(page)
	}

	t2 := time.Since(t1)

	bufferManager.WriteDirtys()
	bufferManager.Ds.CloseFile()

	fmt.Printf("\ntotal tests: 	%d\n", counter)

	fmt.Printf("\ntime cost: 	%d (ms)\n\n", t2.Milliseconds())

	fmt.Printf("BUFFER STATISTICS:\n")
	fmt.Printf("hit   = %d\n", bm.Hit)
	fmt.Printf("miss  = %d\n", bm.Miss)
	fmt.Printf("rate  = %v\n\n", float64(bm.Hit)/float64(counter))

	fmt.Printf("I/O STATISTICS:\n")
	fmt.Printf("total = %d\n", dsm.IOCounter)
	fmt.Printf("read  = %d\n", dsm.Read)
	fmt.Printf("write = %d\n\n", dsm.Write)
}
