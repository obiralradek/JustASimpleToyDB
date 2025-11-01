package storage

import (
	"encoding/binary"
	"fmt"
)

const tupleHdrSize = 12 // 8 (xmin) + 2 (flags) + 2 (reserved)

type Tuple struct {
	Xmin  uint64
	Flags uint16
	Data  []byte
}

func encodeTupleHeader(buf []byte, xmin uint64, flags uint16) {
	binary.LittleEndian.PutUint64(buf[0:8], xmin)
	binary.LittleEndian.PutUint16(buf[8:10], flags)
}

func decodeTupleHeader(buf []byte) (xmin uint64, flags uint16) {
	xmin = binary.LittleEndian.Uint64(buf[0:8])
	flags = binary.LittleEndian.Uint16(buf[8:10])
	return
}

func (p *Page) GetTuple(slotIdx int) (*Tuple, error) {
	slotCount := int(p.getSlotCount())
	if slotIdx < 0 || slotIdx >= slotCount {
		return nil, fmt.Errorf("slot index out of range")
	}
	slotOffset := PageSize - ((slotIdx + 1) * slotEntrySz)
	offset := int(binary.LittleEndian.Uint32(p.Data[slotOffset : slotOffset+4]))
	length := int(binary.LittleEndian.Uint32(p.Data[slotOffset+4 : slotOffset+8]))
	if offset+length > PageSize {
		return nil, fmt.Errorf("corrupt slot (out of bounds)")
	}
	hdrOff := offset
	if length < tupleHdrSize {
		return nil, fmt.Errorf("tuple too small")
	}
	xmin, flags := decodeTupleHeader(p.Data[hdrOff : hdrOff+tupleHdrSize])
	payloadLen := length - tupleHdrSize
	out := make([]byte, payloadLen)
	copy(out, p.Data[hdrOff+tupleHdrSize:hdrOff+tupleHdrSize+payloadLen])
	return &Tuple{Xmin: xmin, Flags: flags, Data: out}, nil
}

func (p *Page) InsertTouple(payload []byte, xmin uint64, flags uint16) (int, error) {
	n := len(payload) + tupleHdrSize
	if !p.CanInsert(n) {
		return -1, fmt.Errorf("not enough space in page %d: need %d, have %d", p.ID, n+slotEntrySz, p.availableSpace())
	}

	dataEnd := int(p.getDataEnd())

	encodeTupleHeader(p.Data[dataEnd:dataEnd+tupleHdrSize], xmin, flags)

	copy(p.Data[dataEnd+tupleHdrSize:dataEnd+tupleHdrSize+len(payload)], payload)
	newDataEnd := dataEnd + n
	p.setDataEnd(uint32(newDataEnd))

	slotCount := int(p.getSlotCount())
	slotOffset := PageSize - ((slotCount + 1) * slotEntrySz)
	binary.LittleEndian.PutUint32(p.Data[slotOffset:slotOffset+4], uint32(dataEnd))
	binary.LittleEndian.PutUint32(p.Data[slotOffset+4:slotOffset+8], uint32(n))

	p.setSlotCount(uint32(slotCount + 1))
	return slotCount, nil
}
