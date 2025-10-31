package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type Pager struct {
	file *os.File
	path string
}

func NewPager(path string) *Pager {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(fmt.Sprintf("failed to create directory %s: %v", dir, err))
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed to open pager file: %v", err))
	}
	return &Pager{file: f, path: path}
}

func (p *Pager) WritePage(page *Page) error {
	offset := int64(page.ID) * PageSize
	_, err := p.file.WriteAt(page.Data, offset)
	return err
}

func (p *Pager) ReadPage(id uint64) (*Page, error) {
	buf := make([]byte, PageSize)
	offset := int64(id) * PageSize
	n, err := p.file.ReadAt(buf, offset)
	if err != nil {
		// return error as-is (caller can interpret)
		return nil, err
	}
	if n != PageSize {
		return nil, fmt.Errorf("short read: %d bytes", n)
	}
	return pageFromBuf(id, buf), nil
}

func (p *Pager) NumPages() (uint64, error) {
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if size%PageSize != 0 {
		// treat partial page as error for now
		return 0, fmt.Errorf("file size not multiple of page size: %d", size)
	}
	return uint64(size / PageSize), nil
}

func (p *Pager) Close() error {
	return p.file.Close()
}
