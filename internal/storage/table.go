package storage

import (
	"fmt"
	"io"
	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/engine/rowcodec"
)

type Table struct {
	name   string
	schema *catalog.TableSchema
	pager  *Pager
}

// NewTable opens/creates a table file and returns Table
func NewTable(name string, path string, schema *catalog.TableSchema) (*Table, error) {
	p := NewPager(path)
	return &Table{name: name, pager: p, schema: schema}, nil
}

// close underlying pager
func (t *Table) Close() error {
	return t.pager.Close()
}

// InsertRow appends a row into last page or allocates a new page
func (t *Table) InsertRow(values []any) error {
	data, err := rowcodec.EncodeRow(t.schema, values)
	numPages, err := t.pager.NumPages()
	if err != nil {
		return err
	}

	var page *Page
	// if no pages yet, create first page
	if numPages == 0 {
		page = NewEmptyPage(0)
	} else {
		// read last page
		pageID := numPages - 1
		pg, err := t.pager.ReadPage(pageID)
		if err != nil {
			// if read fails because file shorter, create new
			page = NewEmptyPage(pageID)
		} else {
			page = pg
		}
	}

	// try to insert
	if page.CanInsert(len(data)) {
		_, err := page.InsertRecord(data)
		if err != nil {
			return err
		}
		// write page back
		if err := t.pager.WritePage(page); err != nil {
			return err
		}
		return nil
	}

	// not enough space, allocate next page
	newPageID := numPages
	newPage := NewEmptyPage(newPageID)
	if !newPage.CanInsert(len(data)) {
		return fmt.Errorf("row too large for empty page (size %d, max %d)", len(data), PageSize-pageHdrSize-slotEntrySz)
	}
	_, err = newPage.InsertRecord(data)
	if err != nil {
		return err
	}
	if err := t.pager.WritePage(newPage); err != nil {
		return err
	}
	return nil
}

// ReadAllRows iterates all pages and returns all rows in order
func (t *Table) ReadAllRows() ([]any, error) {
	numPages, err := t.pager.NumPages()
	if err != nil {
		// if file doesn't exist or empty, return empty result
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	out := make([]any, 0, 64)
	for i := uint64(0); i < numPages; i++ {
		pg, err := t.pager.ReadPage(i)
		if err != nil {
			return nil, err
		}
		slots := int(pg.getSlotCount())
		for s := 0; s < slots; s++ {
			rec, err := pg.GetRecord(s)
			if err != nil {
				return nil, err
			}
			data, err := rowcodec.DecodeRow(t.schema, rec)
			out = append(out, data)
		}
	}
	return out, nil
}
