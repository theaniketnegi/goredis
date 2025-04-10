package store

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type StoreValue struct {
	Value  string
	Expiry *time.Time
}
type InMemoryStore struct {
	Storage map[string]StoreValue
	mu      sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		Storage: make(map[string]StoreValue),
	}
	s.BackgroundKeyCleanup(15000)
	return s
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

	value, ok := s.Storage[key]

	if hasExpired(value.Expiry) {
		return StoreValue{}, false
	}

	return value, ok
}

func (s *InMemoryStore) Set(key string, value string, expiry *time.Time, nx bool, xx bool, ttl bool, get bool) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldVal, ok := s.Storage[key]
	if hasExpired(oldVal.Expiry) {
		delete(s.Storage, key)
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

	s.Storage[key] = StoreValue{Value: value, Expiry: expiry}
	if get {
		if ok {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(oldVal.Value), oldVal.Value)
		} else {
			return "_\r\n"
		}
	}

	return "+OK\r\n"
}

func (s *InMemoryStore) GetKeys(pattern string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var matchedKeys []string
	for k := range s.Storage {
		if match, _ := regexp.MatchString(pattern, k); match {
			matchedKeys = append(matchedKeys, k)
		}
	}

	return matchedKeys
}

func (s *InMemoryStore) Increment(key string, by int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storeValue, ok := s.Storage[key]

	if !ok {
		s.Storage[key] = StoreValue{Value: fmt.Sprintf("%d", by)}
		return by, nil
	}

	if hasExpired(storeValue.Expiry) {
		delete(s.Storage, key)
		s.Storage[key] = StoreValue{Value: fmt.Sprintf("%d", by)}
		return by, fmt.Errorf("key expired")
	}

	value, err := strconv.ParseInt(storeValue.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer or out of range")
	}

	if (by > 0 && value > math.MaxInt64 - by) || (by < 0 && value < math.MinInt64 - by) {
		return 0, fmt.Errorf("increment or decrement would overflow")
	}

	value += by
	s.Storage[key] = StoreValue{Value: fmt.Sprintf("%d", value), Expiry: storeValue.Expiry}
	return value, nil
}

func (s *InMemoryStore) NumKeyExists(keys []string, shouldDelete bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0

	for _, k := range keys {
		if _, ok := s.Storage[k]; ok {
			if shouldDelete {
				delete(s.Storage, k)
			}
			count++
		}
	}

	return count
}

func (s *InMemoryStore) BackgroundKeyCleanup(sleepTime time.Duration) {
	go func() {
		for {
			time.Sleep(sleepTime * time.Millisecond)

			s.mu.Lock()
			for k, v := range s.Storage {
				if hasExpired(v.Expiry) {
					delete(s.Storage, k)
				}
			}
			s.mu.Unlock()
		}
	}()
}
