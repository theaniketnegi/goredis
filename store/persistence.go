package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Persistence struct {
	persistentFile string
	mu             sync.Mutex
	memory         *InMemoryStore
	isBGSaving     bool
	lastSaveTime   int64
}

func NewPersistence(kvStore *InMemoryStore, fileDir string) (*Persistence, error) {
	p := &Persistence{
		persistentFile: fileDir,
		memory:         kvStore,
		lastSaveTime:   0,
	}

	defer func() {
		if info, err := os.Stat(fileDir); err == nil {
			p.lastSaveTime = info.ModTime().Unix()
		}
	}()

	err := p.loadFileData()

	if errors.Is(err, os.ErrNotExist) {
		dir := filepath.Dir(fileDir)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, errors.New("error creating persistent file (directory)")
		}

		_, err := os.Create(fileDir)

		if err != nil {
			return nil, errors.New("error creating persistent file: " + fileDir)
		}

		return p, nil
	}

	if err != nil {
		if err == io.EOF {
			return p, nil
		}
		return nil, err
	}

	return p, nil
}

func (p *Persistence) LoadFileContents() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, err := os.Open(p.persistentFile)

	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	_, err = b.ReadFrom(file)

	if err != nil {
		return "", err
	}

	return b.String(), nil
}

func (p *Persistence) LoadDataFromFile(content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rdr := strings.NewReader(content)
	dec := gob.NewDecoder(rdr)
	err := dec.Decode(p.memory)

	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	return nil
}

func (p *Persistence) loadFileData() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, err := os.Open(p.persistentFile)

	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)

	err = dec.Decode(p.memory)

	if err != nil {
		return err
	}
	return nil
}

func (p *Persistence) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isBGSaving {
		return errors.New("another save in progress")
	}

	persistentFileDir := filepath.Dir(p.persistentFile)
	tmpfile, err := os.CreateTemp(persistentFileDir, filepath.Base(p.persistentFile))
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name())

	encoder := gob.NewEncoder(tmpfile)
	err = encoder.Encode(p.memory)

	if err != nil {
		tmpfile.Close()
		return err
	}

	tmpfile.Close()
	err = os.Rename(tmpfile.Name(), p.persistentFile)
	if err != nil {
		return err
	}

	p.lastSaveTime = time.Now().Unix()
	return nil
}

func (p *Persistence) BGSave() error {
	p.mu.Lock()

	if p.isBGSaving {
		p.mu.Unlock()
		return errors.New("background save is already running")
	}
	p.isBGSaving = true
	p.mu.Unlock()

	go func() {
		p.mu.Lock()
		p.isBGSaving = false
		p.mu.Unlock()
		p.Save()
	}()
	return nil
}

func (p *Persistence) LastSave() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lastSaveTime
}
