package store

import (
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Persistence struct {
	persistentFile string
	mu             sync.Mutex
	memory         *InMemoryStore
}

func NewPersistence(kvStore *InMemoryStore, fileDir string) (*Persistence, error) {
	p := &Persistence{
		persistentFile: fileDir,
		memory:         kvStore,
	}

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

	persistentFileDir := filepath.Dir(p.persistentFile)
	tmpfile, err := os.CreateTemp(persistentFileDir, filepath.Base(p.persistentFile))
	if err != nil {
		return err
	}
	defer os.Remove(persistentFileDir + "/" + tmpfile.Name())

	encoder := gob.NewEncoder(tmpfile)
	err = encoder.Encode(p.memory)

	if err != nil {
		tmpfile.Close()
		return err
	}

	tmpfile.Close()

	err = os.Rename(persistentFileDir+"/"+tmpfile.Name(), p.persistentFile)
	if err != nil {
		return err
	}
	return nil
}
