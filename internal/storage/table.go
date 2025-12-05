package storage

import (
	"fmt"
	"io"
	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/engine/rowcodec"
	"path/filepath"
)

type Table struct {
	name    string
	schema  *catalog.TableSchema
	pager   *Pager
	Indexes map[string]*Index
	dataDir string // directory where table and index files are stored
}

// NewTable opens/creates a table file and returns Table
func NewTable(name string, path string, schema *catalog.TableSchema) (*Table, error) {
	p := NewPager(path)
	dataDir := filepath.Dir(path)
	t := &Table{
		name:    name,
		pager:   p,
		schema:  schema,
		Indexes: make(map[string]*Index),
		dataDir: dataDir,
	}
	// Load existing indexes
	if err := t.loadIndexes(); err != nil {
		return nil, fmt.Errorf("failed to load indexes: %w", err)
	}
	return t, nil
}

// loadIndexes loads all indexes defined in the schema
func (t *Table) loadIndexes() error {
	for indexName := range t.schema.Indexes {
		indexPath := filepath.Join(t.dataDir, fmt.Sprintf("%s_%s.idx", t.name, indexName))
		pager := NewPager(indexPath)
		idx, err := NewIndex(pager)
		if err != nil {
			// NewIndex can fail if:
			// 1. File doesn't exist (but NewPager creates it, so this shouldn't happen)
			// 2. File exists but size is not a multiple of PageSize (corrupted/partial write)
			// 3. File is empty (0 bytes) - this should work (returns 0 pages, creates root)
			// For corrupted files, we'll log but continue - the index will be created on first insert
			// This allows the system to recover from partial writes
			continue
		}
		t.Indexes[indexName] = idx
	}
	return nil
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
	for indexName, idx := range t.schema.Indexes {
		colIdx, err := t.ResolveColumn(idx.ColumnName)
		if err != nil {
			continue
		}
		// Get or create index
		index, ok := t.Indexes[indexName]
		if !ok {
			// Index not loaded, try to load it
			// This can happen if loadIndexes() skipped it or if it was added after table was opened
			indexPath := filepath.Join(t.dataDir, fmt.Sprintf("%s_%s.idx", t.name, indexName))
			pager := NewPager(indexPath)
			var loadErr error
			index, loadErr = NewIndex(pager)
			if loadErr != nil {
				// NewIndex failed - this could be because:
				// 1. File is corrupted (size not multiple of PageSize)
				// 2. Some other I/O error
				// For now, we'll return an error rather than silently failing
				return fmt.Errorf("failed to load index %q: %v", indexName, loadErr)
			}
			t.Indexes[indexName] = index
		}
		b, err := rowcodec.EncodeValue(t.schema, colIdx, values[colIdx])
		if err != nil {
			return fmt.Errorf("failed to encode value for index %q: %v", indexName, err)
		}
		if err := index.Insert(b, tid); err != nil {
			return fmt.Errorf("failed to insert into index %q: %v", indexName, err)
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
			if err != nil {
				return nil, err
			}
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

	indexPath := filepath.Join(t.dataDir, fmt.Sprintf("%s_%s.idx", t.name, name))
	pager := NewPager(indexPath)
	idx, err := NewIndex(pager)
	if err != nil {
		return err
	}

	// Populate index from existing rows by iterating pages and slots directly
	// to get correct TIDs
	numPages, err := t.pager.NumPages()
	if err != nil {
		if err == io.EOF {
			// Empty table, just store the empty index
			t.Indexes[name] = idx
			return nil
		}
		return fmt.Errorf("failed to get page count: %w", err)
	}

	for pageID := uint64(0); pageID < numPages; pageID++ {
		pg, err := t.pager.ReadPage(pageID)
		if err != nil {
			return fmt.Errorf("failed to read page %d: %w", pageID, err)
		}
		slots := int(pg.getSlotCount())
		for slotID := 0; slotID < slots; slotID++ {
			rec, err := pg.GetRecord(slotID)
			if err != nil {
				// Skip corrupted records
				continue
			}
			row, err := rowcodec.DecodeRow(t.schema, rec)
			if err != nil {
				// Skip rows that can't be decoded
				continue
			}
			tid := TID{PageID: pageID, SlotID: uint32(slotID)}
			b, err := rowcodec.EncodeValue(t.schema, colIdx, row[colIdx])
			if err != nil {
				return fmt.Errorf("failed to encode value: %w", err)
			}
			if err := idx.Insert(b, tid); err != nil {
				return fmt.Errorf("failed to insert into index: %w", err)
			}
		}
	}

	// Store index in cache
	t.Indexes[name] = idx
	return nil
}

func (t *Table) GetIndex(name string) (*Index, error) {
	// Check cache first
	if idx, ok := t.Indexes[name]; ok {
		return idx, nil
	}
	// Try to load it
	indexPath := filepath.Join(t.dataDir, fmt.Sprintf("%s_%s.idx", t.name, name))
	pager := NewPager(indexPath)
	idx, err := NewIndex(pager)
	if err != nil {
		return nil, err
	}
	t.Indexes[name] = idx
	return idx, nil
}
