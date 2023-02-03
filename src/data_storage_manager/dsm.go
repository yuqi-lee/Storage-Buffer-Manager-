package data_storage_manager

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
)

const (
	FrameSize int32 = 4096
	MaxPages  int32 = 50000
)

var (
	Read  int32
	Write int32
)

type BFrame struct {
	Field [FrameSize]byte
}

type DSMgr struct {
	pagesStart int32
	numPages   int32
	pages      [MaxPages]int32
	currFile   *os.File
}

func (dsm *DSMgr) InitFile(filename string) error {
	//tips: distinguish binary 0 from character '0'
	var zeros = [MaxPages + 4]byte{'0'}
	for i := 0; i < 4; i++ {
		zeros[i] = 0
	}

	// overwrite the raw file
	err := ioutil.WriteFile(filename, zeros[:], 0644)
	return err
}

func (dsm *DSMgr) OpenFile(filename string) error {
	var err error
	dsm.currFile, err = os.Open(filename)
	if err != nil {
		return err
	}

	var numPagesBytes [4]byte
	_, err = dsm.currFile.Read(numPagesBytes[:])
	if err != nil {
		return err
	}
	dsm.numPages = bytes2Int32(numPagesBytes[:]) //fread(&numPages, 4, 1, currFile)

	dsm.pagesStart = ((MaxPages*1+1*4-1)/FrameSize + 1) * FrameSize

	var pageBytes [MaxPages]byte
	var i int32
	dsm.currFile.Read(pageBytes[0:dsm.numPages])
	for i < dsm.numPages {
		if pageBytes[i] == '0' {
			dsm.pages[i] = 0
		} else if pageBytes[i] == '1' {
			dsm.pages[i] = 1
		} else {
			return errors.New("Unexpected error in dbf file.")
		}

		i++
	}

	log.Printf("numPages: 	%v\n", dsm.numPages)
	log.Printf("pagesStart: %v\n", dsm.pagesStart)
	log.Println("Data Storage Manager: Open DBF-file success.")
	return nil
}

func (dsm *DSMgr) CloseFile() error {
	dsm.DSeek(0, 0)                                          //page number at the beginning
	_, err := dsm.currFile.Write(int32ToBytes(dsm.numPages)) //fwrite(&numPages, 4, 1, currFile)
	if err != nil {
		return err
	}

	var pagesbyte [MaxPages]byte
	var i int32 = 0
	for ; i < dsm.numPages; i++ {
		if dsm.pages[i] == 0 {
			pagesbyte[i] = '0'
		} else {
			pagesbyte[i] = '1'
		}
	}

	_, err = dsm.currFile.Write(pagesbyte[:]) // fwrite(pagesbyte, numPages, 1, currFile)
	if err != nil {
		return err
	}

	return nil
}

func (dsm *DSMgr) ReadPage(pageID int32) (*BFrame, error) {
	bf := &BFrame{}

	var pageOffset [4]byte
	_, err := dsm.DSeek(dsm.pagesStart, pageID*4)
	if err != nil {
		return bf, err
	}
	dsm.currFile.Read(pageOffset[:]) //fread(&p, 4, 1, currFile);
	p := bytes2Int32(pageOffset[:])

	_, err = dsm.DSeek(dsm.pagesStart, p)
	if err != nil {
		return bf, err
	}
	dsm.currFile.Read(bf.Field[:]) //fread(bf.field, FrameSize, 1, dsm.currFile)
	Read += 1

	return bf, nil
}

func (dsm *DSMgr) WritePage(pageID int32, f *BFrame) error {
	_, err := dsm.DSeek(dsm.pagesStart, pageID*4)
	if err != nil {
		return err
	}

	var pageOffset [4]byte
	dsm.currFile.Read(pageOffset[:]) // fread(&p, 4, 1, currFile);
	p := bytes2Int32(pageOffset[:])

	_, err = dsm.DSeek(dsm.pagesStart, p)
	if err != nil {
		return err
	}
	dsm.currFile.Write(f.Field[:])
	Write += 1
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
	if dsm.pages[index] == 0 && use_bit == 1 { //Start a new page.
		p := ((MaxPages*4-1)/FrameSize+1)*FrameSize + FrameSize*index

		_, err := dsm.DSeek(dsm.pagesStart, index*4)
		if err != nil {
			return nil
		}
		_, err = dsm.currFile.Write(int32ToBytes(p)) // fwrite(&p, 4, 1, dsm.currFile) //写入指针
		if err != nil {
			return nil
		}

		var b BFrame = BFrame{}
		_, err = dsm.DSeek(dsm.pagesStart, p)
		if err != nil {
			return nil
		}
		_, err = dsm.currFile.Write(b.Field[:]) //fwrite(b.field, FRAMESIZE, 1, currFile)
		if err != nil {
			return nil
		}
	}
	dsm.pages[index] = use_bit

	return nil
}

func (dsm *DSMgr) GetUse(index int32) int32 {
	return dsm.pages[index]
}
