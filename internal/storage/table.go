package storage

import (
	"fmt"
	"io"
	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/engine/rowcodec"
)

type Table struct {
	name    string
	schema  *catalog.TableSchema
	pager   *Pager
	Indexes map[string]*Index
}

// NewTable opens/creates a table file and returns Table
func NewTable(name string, path string, schema *catalog.TableSchema) (*Table, error) {
	p := NewPager(path)
	return &Table{
		name:    name,
		pager:   p,
		schema:  schema,
		Indexes: make(map[string]*Index),
	}, nil
}

// close underlying pager
func (t *Table) Close() error {
	return t.pager.Close()
}

// InsertRow appends a row into last page or allocates a new page
func (t *Table) InsertRow(values []any) error {
	data, err := rowcodec.EncodeRow(t.schema, values)
	if err != nil {
		return err
	}

	numPages, err := t.pager.NumPages()
	if err != nil {
		return err
	}

	var page *Page
	if numPages == 0 {
		page = NewEmptyPage(0)
	} else {
		pageID := numPages - 1
		pg, err := t.pager.ReadPage(pageID)
		if err != nil {
			page = NewEmptyPage(pageID)
		} else {
			page = pg
		}
	}

	if !page.CanInsert(len(data) + tupleHdrSize) {
		newPageID := numPages
		page = NewEmptyPage(newPageID)
	}

	// Insert row as tuple
	slotID, err := page.InsertTouple(data, 0, TupleFlagNormal)
	if err != nil {
		return err
	}

	if err := t.pager.WritePage(page); err != nil {
		return err
	}

	// Build TID for this row
	tid := TID{PageID: page.ID, SlotID: uint32(slotID)}

	// Update indexes
	for colName, idx := range t.Indexes {
		colIdx, err := t.ResolveColumn(colName)
		if err != nil {
			continue
		}
		b, err := rowcodec.EncodeValue(t.schema, colIdx, values[colIdx])
		if err := idx.Insert(b, tid); err != nil {
			return fmt.Errorf("failed to insert into index %q: %v", colName, err)
		}
	}

	return nil
}

// ReadAllRows iterates all pages and returns all rows in order
func (t *Table) ReadAllRows() ([][]any, error) {
	numPages, err := t.pager.NumPages()
	if err != nil {
		// if file doesn't exist or empty, return empty result
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	out := make([][]any, 0, 64)
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

func (t *Table) ResolveColumns(requested []string) ([]int, []string, error) {
	// If one requested that is * resolve into all columns
	if len(requested) == 1 && requested[0] == "*" {
		idxs := make([]int, len(t.schema.Columns))
		names := make([]string, len(t.schema.Columns))
		for i, c := range t.schema.Columns {
			idxs[i] = i
			names[i] = c.Name
		}
		return idxs, names, nil
	}

	// Otherwise, match requested names
	idxs := make([]int, len(requested))
	for i, name := range requested {
		found := false
		for j, col := range t.schema.Columns {
			if col.Name == name {
				idxs[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("unknown column %q", name)
		}
	}
	return idxs, requested, nil
}

func (t *Table) ResolveColumn(column string) (int, error) {
	for j, col := range t.schema.Columns {
		if col.Name == column {
			return j, nil
		}
	}
	return 0, fmt.Errorf("unknown column %q", column)
}

func (t *Table) GetTupleByTID(tid TID) (*Tuple, error) {
	pg, err := t.pager.ReadPage(tid.PageID)
	if err != nil {
		return nil, err
	}
	return pg.GetTuple(int(tid.SlotID))
}

func (t *Table) CreateIndex(name, column string) error {
	colIdx := -1
	for i, c := range t.schema.Columns {
		if c.Name == column {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("column %q does not exist", column)
	}

	pager := NewPager(fmt.Sprintf("data/%s_%s.idx", t.name, name))
	idx, err := t.NewIndex(pager)
	if err != nil {
		return err
	}
	idx.Column = column
	idx.Name = name

	// populate index from existing rows
	rows, _ := t.ReadAllRows()
	for pageID, row := range rows {
		tid := TID{PageID: 0, SlotID: uint32(pageID)} // simplification
		b, err := rowcodec.EncodeValue(t.schema, colIdx, row[colIdx])
		if err != nil {
			return err
		}
		idx.Insert(b, tid)
	}

	if t.Indexes == nil {
		t.Indexes = make(map[string]*Index)
	}
	t.Indexes[name] = idx
	return nil
}
