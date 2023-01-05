package buffer_manager

const (
	BufferSize int32 = 1
)

type BCB struct {
	page_id  int32
	frame_id int32
	count    int32
	dirty    int32
	next     *BCB
}

type NewPage struct {
	page_id  int32
	frame_id int32
}

type BMgr struct {
	ftop [BufferSize]int32
	ptof [BufferSize]*BCB
}

func (bm *BMgr) FixPage(page_id, prot int32) (int32, error) {
	var res int32
	return res, nil
}

func (bm *BMgr) FixNewPage() (*NewPage, error) {
	np := &NewPage{}
	return np, nil
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
