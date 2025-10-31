package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	PageSize    = 16 * 1024 // 16KB
	pageHdrSize = 16        // 8 (id) + 4 (dataEnd) + 4 (slotCount)
	slotEntrySz = 8         // 4 (offset) + 4 (length)
)

type Page struct {
	ID   uint64
	Data []byte // fixed length PageSize
}

func NewEmptyPage(id uint64) *Page {
	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(buf[0:8], id)
	// dataEnd defaults to header size
	binary.LittleEndian.PutUint32(buf[8:12], uint32(pageHdrSize))
	// slotCount defaults to 0 -> bytes 12:16 zero
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

// InsertRecord writes payload into page and creates new slot entry, returns slot index (0-based)
func (p *Page) InsertRecord(payload []byte) (int, error) {
	n := len(payload)
	if !p.CanInsert(n) {
		return -1, fmt.Errorf("not enough space in page %d: need %d, have %d", p.ID, n+slotEntrySz, p.availableSpace())
	}

	dataEnd := int(p.getDataEnd())
	// write payload at dataEnd
	copy(p.Data[dataEnd:dataEnd+n], payload)
	newDataEnd := dataEnd + n
	p.setDataEnd(uint32(newDataEnd))

	// compute slot position (slot entries stored from end backwards)
	slotCount := int(p.getSlotCount())
	slotOffset := PageSize - ((slotCount + 1) * slotEntrySz)

	// write slot entry: offset (uint32), length (uint32)
	binary.LittleEndian.PutUint32(p.Data[slotOffset:slotOffset+4], uint32(dataEnd))
	binary.LittleEndian.PutUint32(p.Data[slotOffset+4:slotOffset+8], uint32(n))

	p.setSlotCount(uint32(slotCount + 1))
	return slotCount, nil
}

// GetRecord returns record bytes for given slot index (0-based)
func (p *Page) GetRecord(slotIdx int) ([]byte, error) {
	slotCount := int(p.getSlotCount())
	if slotIdx < 0 || slotIdx >= slotCount {
		return nil, fmt.Errorf("slot index out of range")
	}
	slotOffset := PageSize - ((slotIdx + 1) * slotEntrySz)
	offset := binary.LittleEndian.Uint32(p.Data[slotOffset : slotOffset+4])
	length := binary.LittleEndian.Uint32(p.Data[slotOffset+4 : slotOffset+8])
	if int(offset)+int(length) > PageSize {
		return nil, fmt.Errorf("corrupt slot (out of bounds)")
	}
	// copy to new slice to avoid exposing page backing buffer
	out := make([]byte, length)
	copy(out, p.Data[offset:uint32(offset)+length])
	return out, nil
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
