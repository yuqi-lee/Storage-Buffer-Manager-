package buffer_manager

import (
	"errors"
	"log"

	dsm "adbslab.com/data_storage_manager"
)

const (
	BufferSize int32 = 1
)

var (
	Hit  int32
	Miss int32
)

type BCB struct {
	page_id  int32
	frame_id int32
	count    int32
	dirty    int32
	next     *BCB
}

type NewPage struct {
	Page_id  int32
	frame_id int32
}

type BMgr struct {
	Ds   dsm.DSMgr
	ftop [BufferSize]int32
	ptof [BufferSize]*BCB
}

func (bm *BMgr) NewBCB(page_id int32) (int32, error) {
	var frame_id int32
	var err error

	return frame_id, err
}

func (bm *BMgr) FixPage(page_id, prot int32) (int32, error) {
	var res int32
	var err error
	if page_id >= 50000 || page_id < 0 {
		return 0, errors.New("invalid page_id.")
	}

	if prot != 0 {
		log.Println("invalid prot.")
		prot = 0
	}

	bcb := bm.ptof[Hash(page_id)]
	for bcb != nil && bcb.next != nil && bcb.page_id != page_id {
		bcb = bcb.next
	}

	// The page to be read and written is already in the cache.
	// The lru table cache hit should be updated.
	if bcb != nil && bcb.page_id == page_id {

		Hit += 1
		res = bcb.frame_id
		//TODO:
		//lru.remove(frame_id)
		//lru.push_front(frame_id)
		bcb.count = bcb.count + 1
	} else {
		Miss += 1
		res, err = bm.NewBCB(page_id)
		if err != nil {
			return 0, errors.New("Create new BCB failed.")
		}
		var bf *dsm.BFrame
		bf, err = bm.Ds.ReadPage(page_id)
		//TODO:
		//lru.remove(frame_id)
		//lru.push_front(frame_id)
		bf.Field[0] = 0
	}
	return res, err
}

func (bm *BMgr) FixNewPage() (*NewPage, error) {
	np := &NewPage{}
	var err error

	var index int32
	for index < bm.Ds.GetNumPages() { //Find first free page
		if bm.Ds.GetUse(index) == 0 {
			break
		}
		index++
	}

	if index == bm.Ds.GetNumPages() { // All pages are used, and the dbf file needs to be increased
		err := bm.Ds.IncNumPages()
		if err != nil {
			return np, err
		}
		bm.Ds.SetUse(index, 1)
		if err != nil {
			return np, err
		}
	}
	np.Page_id = index
	np.frame_id, err = bm.FixPage(index, 0)

	return np, err
}

func (bm *BMgr) UnfixPage(pageID int32) error {
	return nil
}

func (bm *BMgr) NumFreeFrames() int32 {
	var res int32 = 0
	for i := 0; i < int(BufferSize); i++ {
		if bm.ftop[i] == -1 {
			res += 1
		}
	}
	return res
}

// Internal Functions

func (bm *BMgr) SelectVictim() (int32, error) {
	var res int32
	return res, nil
}

func (bm *BMgr) RemoveBCB(ptr *BCB, pageID int32) error {
	return nil
}

func (bm *BMgr) RemoveLRUEle(frameID int32) error {
	return nil
}

func (bm *BMgr) SetDirty(frameID int32) error {
	return nil
}

func (bm *BMgr) UnsetDirty(frameID int32) error {
	return nil
}

func (bm *BMgr) WriteDirtys() error {
	return nil
}

func (bm *BMgr) PrintFrame(frameID int32) error {
	return nil
}
