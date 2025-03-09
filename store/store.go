package store

import (
	"fmt"
	"sync"
	"time"
)

type StoreValue struct {
	Value  string
	Expiry *time.Time
}
type InMemoryStore struct {
	storage map[string]StoreValue
	mu      sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		storage: make(map[string]StoreValue),
	}
}

func hasExpired(expiry *time.Time) bool {
	if expiry == nil {
		return false
	}

	timeLeft := int64(time.Until(*expiry).Seconds())

	return timeLeft <= 0
}

func (s *InMemoryStore) Get(key string) (StoreValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.storage[key]

	if hasExpired(value.Expiry) {
		return StoreValue{}, false
	}

	return value, ok
}

func (s *InMemoryStore) Set(key string, value string, expiry *time.Time, nx bool, xx bool, ttl bool, get bool) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldVal, ok := s.storage[key]
	if hasExpired(oldVal.Expiry) {
		ok = false
	}
	if nx && ok {
		return "_\r\n"
	}

	if xx && !ok {
		return "_\r\n"
	}

	if ttl {
		if !ok {
			expiry = nil
		} else {
			expiry = oldVal.Expiry
		}
	}

	s.storage[key] = StoreValue{Value: value, Expiry: expiry}
	if get {
		if ok {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(oldVal.Value), oldVal.Value)
		} else {
			return "_\r\n"
		}
	}

	return "+OK\r\n"
}
