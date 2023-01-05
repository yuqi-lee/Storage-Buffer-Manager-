package data_storage_manager

import (
	"bufio"
	"errors"
	"log"
	"os"
)

const (
	FrameSize int32 = 4096
	MaxPages  int32 = 50000
)

type bFrame struct {
	field [FrameSize]byte
}

type DSMgr struct {
	pagesStart int32
	numPages   int32
	pages      [MaxPages]int32
	currFile   *os.File
}

func (dsm *DSMgr) OpenFile(filename string) error {
	log.Println("Open dbf file...")
	var err error
	dsm.currFile, err = os.Open(filename)
	if err != nil {
		return err
	}

	_ = bufio.NewReader(dsm.currFile)

	return nil
}

func (dsm *DSMgr) CloseFile() error {
	return nil
}

func (dsm *DSMgr) ReadPage(pageID int32) (*bFrame, error) {
	f := &bFrame{}
	return f, nil
}

func (dsm *DSMgr) WritePage(pageID int32, f *bFrame) error {
	return nil
}

func (dsm *DSMgr) DSeek(offset, pos int32) (int32, error) {
	res, err := dsm.currFile.Seek(int64(offset), int(pos))
	return int32(res), err
}

func (dsm *DSMgr) IncNumPages() error {
	if dsm.numPages >= MaxPages {
		return errors.New("exceeded maximum number of pages allowed for experiment.")
	} else {
		dsm.numPages++
		return nil
	}
}

func (dsm *DSMgr) GetNumPages() int32 {
	return dsm.numPages
}

func (dsm *DSMgr) SetUse(index, use_bit int32) error {
	// TODO:

	if dsm.pages[index] == 0 && use_bit == 1 { //新开辟
		p := ((MaxPages*4-1)/FrameSize+1)*FrameSize + FrameSize*index

		dsm.DSeek(dsm.pagesStart, index*4)
		// fwrite(&p, 4, 1, dsm.currFile) //写入指针
		dsm.DSeek(dsm.pagesStart, p)
		//var b bFrame
		//memset(b.field, 0, FRAMESIZE)
		//fwrite(b.field, FRAMESIZE, 1, currFile)
	}
	dsm.pages[index] = use_bit

	return nil
}

func (dsm *DSMgr) GetUse(index int32) int32 {
	return dsm.pages[index]
}
