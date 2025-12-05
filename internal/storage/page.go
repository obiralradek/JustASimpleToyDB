package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	PageSize    = 16 * 1024 // 16KB
	pageHdrSize = 20        // 8 (id) + 4 (dataEnd) + 4 (slotCount) + 2 (flags) + 2 (padding)
	slotEntrySz = 8         // 4 (offset) + 4 (length)
)

type Page struct {
	ID   uint64
	Data []byte // fixed length PageSize
}

func NewEmptyPage(id uint64) *Page {
	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(buf[0:8], id)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(pageHdrSize))
	// slotCount already zero
	binary.LittleEndian.PutUint16(buf[16:18], PageTypeHeap)
	return &Page{ID: id, Data: buf}
}

func pageFromBuf(id uint64, buf []byte) *Page {
	return &Page{ID: id, Data: buf}
}

func (p *Page) getDataEnd() uint32 {
	return binary.LittleEndian.Uint32(p.Data[8:12])
}

func (p *Page) setDataEnd(v uint32) {
	binary.LittleEndian.PutUint32(p.Data[8:12], v)
}

func (p *Page) getSlotCount() uint32 {
	return binary.LittleEndian.Uint32(p.Data[12:16])
}

func (p *Page) setSlotCount(v uint32) {
	binary.LittleEndian.PutUint32(p.Data[12:16], v)
}

// Available free bytes for storing payload + one new slot entry
func (p *Page) availableSpace() int {
	dataEnd := int(p.getDataEnd())
	slotCount := int(p.getSlotCount())
	slotDirBytes := slotCount * slotEntrySz
	return PageSize - dataEnd - slotDirBytes
}

// CanInsert reports whether payload of length n fits (including slot entry)
func (p *Page) CanInsert(n int) bool {
	need := n + slotEntrySz
	return need <= p.availableSpace()
}

// GetRecord returns record bytes for given slot index (0-based)
func (p *Page) GetRecord(slotIdx int) ([]byte, error) {
	t, err := p.GetTuple(slotIdx)
	if err != nil {
		return nil, err
	}
	return t.Data, nil
}

// String yields human friendly representation
func (p *Page) String() string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "Page %d: dataEnd=%d slots=%d\n", p.ID, p.getDataEnd(), p.getSlotCount())
	for i := 0; i < int(p.getSlotCount()); i++ {
		rec, err := p.GetRecord(i)
		if err != nil {
			fmt.Fprintf(b, "  slot %d: ERR %v\n", i, err)
		} else {
			fmt.Fprintf(b, "  slot %d: len=%d content=%q\n", i, len(rec), string(rec))
		}
	}
	return b.String()
}
