package buffer_manager

import (
	"errors"
	"log"

	dsm "adbslab.com/data_storage_manager"
	"github.com/liyue201/gostl/ds/list/bidlist"
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
	lru  bidlist.List[int32]
	buf  [BufferSize]*dsm.BFrame
	ftop [BufferSize]int32
	ptof [BufferSize]*BCB
}

func (bm *BMgr) Init() {
	for i := 0; i < int(BufferSize); i++ {
		bm.ftop[i] = -1
		bm.ptof[i] = nil
		bm.buf[i] = &dsm.BFrame{}
	}
	log.Printf("lru size: %v", bm.lru.Size())
}

func (bm *BMgr) NewBCB(page_id int32) (int32, error) {
	var res int32 = -1
	var err error
	var bcb *BCB = &BCB{page_id: -1, frame_id: -1}

	if bm.lru.Size() < int(BufferSize) {
		// lru
	} else {
		log.Printf("lru size: %v, buffer size: %v", bm.lru.Size(), BufferSize)
		res, err = bm.SelectVictim()
		if err != nil {
			return -1, err
		}
		bm.RemoveLRUEle(res)
	}
	// Find the free space of ftop and use it,
	// but the relationship of ‘BCB -> frame’ needs to be established later
	bcb.page_id = page_id
	suc := false
	//log.Printf("ftop: %v", bm.ftop)
	for i := 0; i < int(BufferSize); i++ {
		if bm.ftop[i] == -1 {
			bm.ftop[i] = page_id
			bcb.frame_id = int32(i)
			res = int32(i)
			suc = true
			break
		}
	}
	if suc == false {
		return -1, errors.New(" lru and ftop conflict.")
	}
	// The new bcb needs to connect to the open chain of the ptof hash table
	// to find the BCB before obtaining the frame to realize the ptof.
	head := bm.ptof[Hash(bcb.page_id)]
	if head == nil {
		bm.ptof[Hash(bcb.page_id)] = bcb
	} else //The 'next' of the last node already points to itself
	{
		for head.next != nil {
			head = head.next
		}
		head.next = bcb
	}
	bcb.count = bcb.count + 1

	bm.lru.PushFront(res)
	return res, nil
}

func (bm *BMgr) FixPage(page_id, prot int32) (int32, error) {
	var res int32
	var err error
	if page_id >= dsm.MaxPages || page_id < 0 {
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

	if bcb != nil && bcb.page_id == page_id {
		// The page to be read and written is already in the cache.
		// The lru table cache hit should be updated.
		Hit += 1
		res = bcb.frame_id
		for n := bm.lru.FrontNode(); n != nil; n = n.Next() {
			if n.Value == bcb.frame_id {
				bm.lru.Remove(n)
				break
			}
		}
		bm.lru.PushFront(bcb.frame_id)
		bcb.count = bcb.count + 1
	} else {
		Miss += 1
		res, err = bm.NewBCB(page_id)
		if err != nil {
			return 0, errors.New("Create new BCB failed." + err.Error())
		}
		var bf *dsm.BFrame
		bf, err = bm.Ds.ReadPage(page_id)
		if err != nil {
			return 0, err
		}
		bm.buf[res] = bf
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
	// log.Printf("free page id is %v", index)
	np.frame_id, err = bm.FixPage(index, 0)

	return np, err
}

func (bm *BMgr) UnfixPage(pageID int32) (int32, error) {
	bcb := bm.ptof[Hash(pageID)]
	for bcb.next != nil && bcb.page_id != pageID {
		bcb = bcb.next
	}

	if bcb == nil {
		return bcb.frame_id, errors.New("Unexcepted error in cache! Try to release pages not in cache.")
	}
	bcb.count = bcb.count - 1
	return bcb.frame_id, nil
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
	var bcb *BCB = nil
	n := bm.lru.BackNode()
	if bm.lru.Size() < 1 {
		return -1, errors.New("lru cache is empty.")
	}

	for n != nil {
		bcb = bm.ptof[Hash(bm.ftop[n.Value])]
		for bcb.frame_id != n.Value && bcb.next != nil {
			bcb = bcb.next
		}
		if bcb.frame_id == n.Value && bcb.count <= 0 {
			break
		}
		n = n.Prev()
	}

	if bcb != nil && (bcb.frame_id != n.Value || bcb.count > 0) {
		return 0, errors.New("All items in the buffer are occupied and cannot be released.")
	} else {
		res = bcb.frame_id
		if bcb.dirty == 1 {
			bm.Ds.WritePage(bcb.page_id, bm.buf[res])
		}
		bm.RemoveBCB(bcb, bcb.page_id)
	}
	return res, nil
}

func (bm *BMgr) RemoveBCB(ptr *BCB, pageID int32) error {
	head := bm.ptof[Hash(ptr.page_id)]
	bm.ftop[ptr.frame_id] = -1
	if head == ptr {
		// The chain header is to be deleted
		bm.ptof[Hash(ptr.page_id)] = ptr.next
	} else {
		// General situation
		for head.next != ptr {
			head = head.next
		}
		head.next = ptr.next
	}
	return nil
}

func (bm *BMgr) RemoveLRUEle(frameID int32) error {
	flag := false
	for n := bm.lru.FrontNode(); n != bm.lru.BackNode(); n = n.Next() {
		if n.Value == frameID {
			bm.lru.Remove(n)
			flag = true
			break
		}
	}

	if flag == false {
		return errors.New("The frame id is not in cache.")
	} else {
		return nil
	}
}

func (bm *BMgr) SetDirty(frameID int32) error {
	bcb := bm.ptof[Hash(bm.ftop[frameID])]
	for bcb.next != nil && bcb.frame_id != frameID {
		bcb = bcb.next
	}

	if bcb.frame_id != frameID {
		return errors.New("Unexpected error in cache content.")
	} else {
		bcb.dirty = 1
		return nil
	}
}

func (bm *BMgr) UnsetDirty(frameID int32) error {
	bcb := bm.ptof[Hash(bm.ftop[frameID])]
	for bcb.next != nil && bcb.frame_id != frameID {
		bcb = bcb.next
	}

	if bcb.frame_id != frameID {
		return errors.New("Unexpected error in cache content.")
	} else {
		bcb.dirty = 0
		return nil
	}
}

func (bm *BMgr) WriteDirtys() error {
	for n := bm.lru.FrontNode(); n != bm.lru.BackNode(); n = n.Next() {
		bcb := bm.ptof[Hash(bm.ftop[n.Value])]
		for bcb.next != nil && bcb.frame_id != n.Value {
			bcb = bcb.next
		}
		if bcb.frame_id != n.Value {
			return errors.New("Unexpected error in cache content: Unable to determine whether to write back or not.")
		} else {
			if bcb.dirty == 1 {
				bm.Ds.WritePage(bcb.page_id, bm.buf[bcb.frame_id])
			}
		}
	}
	return nil
}

func (bm *BMgr) PrintFrame(frameID int32) {
	log.Printf("%v", *bm.buf[frameID])
}
