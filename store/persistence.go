package store

import (
	"encoding/gob"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
)

type Persistence struct {
	persistentFile string
	mu             sync.Mutex
	memory         *InMemoryStore
}

func NewPersistence(kvStore *InMemoryStore, filepath string) (*Persistence, error) {
	p := &Persistence{
		persistentFile: filepath,
		memory:         kvStore,
	}

	err := p.loadFileData()

	if errors.Is(err, os.ErrNotExist) {
		dir := filepath[:strings.LastIndex(filepath, "/")] // Extract directory path
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, errors.New("error creating persistent file (directory)")
		}

		_, err := os.Create(filepath)

		if err != nil {
			return nil, errors.New("error creating persistent file: " + filepath)
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
