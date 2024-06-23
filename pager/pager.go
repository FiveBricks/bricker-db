package pager

import (
	"errors"
	"fmt"
	"os"
)

const PAGE_SIZE = 4096

type Pager struct {
	file   *os.File
	header *DatabaseHeader
}

func NewPager(filePath string) (*Pager, error) {
	file, fileErr := os.OpenFile(filePath, os.O_RDWR, 0644)

	if fileErr != nil {
		if errors.Is(fileErr, os.ErrNotExist) {
			return initPagerInNewFile(filePath)
		}

		return nil, fileErr
	}

	header, headerErr := ReadFromFile(file)
	if headerErr != nil {
		return nil, headerErr
	}

	return &Pager{
		file,
		header,
	}, nil
}

func initPagerInNewFile(filePath string) (*Pager, error) {
	file, fileErr := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if fileErr != nil {
		return nil, fileErr
	}

	header := NewDefaultDatabaseHeader()
	pager := &Pager{
		file,
		header,
	}

	if flushErr := pager.FlushDatabaseHeader(); flushErr != nil {
		return nil, flushErr
	}

	return pager, nil
}

func (p *Pager) CloseFile() error {
	if err := p.file.Sync(); err != nil {
		return err
	}

	return p.file.Close()
}

func (p *Pager) FlushDatabaseHeader() error {
	return p.header.WriteToFile(p.file)
}

func (p *Pager) ReadPage(pageId uint32) ([]byte, error) {
	pageData := make([]byte, PAGE_SIZE)
	offset := PageFileOffset(pageId)
	bytesRead, err := p.file.ReadAt(pageData, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read page data from file: %v", err)
	}
	if bytesRead != PAGE_SIZE {
		return nil, fmt.Errorf("failed to read the entire page, read %d bytes but expected %d bytes", bytesRead, PAGE_SIZE)
	}

	return pageData, nil
}

func (p *Pager) WritePage(pageId uint32, data []byte) error {
	if len(data) != PAGE_SIZE {
		return fmt.Errorf("invalid page size: got %d bytes but expected %d bytes", len(data), PAGE_SIZE)
	}

	offset := PageFileOffset(pageId)

	bytesWritten, err := p.file.WriteAt(data, int64(offset))
	if err != nil {
		return fmt.Errorf("failed to write the page into the file: %v", err)
	}

	if bytesWritten != PAGE_SIZE {
		return fmt.Errorf("failed to write the entire page, wrote %d bytes but expected %d bytes", bytesWritten, PAGE_SIZE)

	}

	return nil
}

func (p *Pager) WriteNewPage(data []byte) (uint32, error) {
	newPageId := p.header.PageCount
	if err := p.WritePage(newPageId, data); err != nil {
		return 0, err
	}

	p.header.PageCount += 1
	if err := p.FlushDatabaseHeader(); err != nil {
		return 0, fmt.Errorf("failed to update database header after writing a new page: %v", err)
	}
	return newPageId, nil
}
