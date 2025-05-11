package store

import (
	"errors"
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

type StoreType int

const (
	StringType StoreType = iota
	ListType
	SetType
	HashType
	SortedSetType
)

type InMemoryStore struct {
	KeyType  map[string]StoreType
	StringKV map[string]StoreValue
	mu       sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		KeyType:  make(map[string]StoreType),
		StringKV: make(map[string]StoreValue),
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

func (s *InMemoryStore) StringGet(key string) (StoreValue, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keyType, ok := s.KeyType[key]
	if !ok {
		return StoreValue{}, false, nil
	}
	if keyType != StringType {
		return StoreValue{}, false, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	value, ok := s.StringKV[key]

	if hasExpired(value.Expiry) {
		delete(s.StringKV, key)
		delete(s.KeyType, key)
		return StoreValue{}, false, nil
	}

	return value, ok, nil
}

func (s *InMemoryStore) StringSet(key string, value string, expiry *time.Time, nx bool, xx bool, ttl bool, get bool) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != StringType {
		return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	}

	oldVal, ok := s.StringKV[key]
	if hasExpired(oldVal.Expiry) {
		delete(s.StringKV, key)
		delete(s.KeyType, key)
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

	s.StringKV[key] = StoreValue{Value: value, Expiry: expiry}
	s.KeyType[key] = StringType
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
	for k := range s.KeyType {
		if match, _ := regexp.MatchString(pattern, k); match {
			matchedKeys = append(matchedKeys, k)
		}
	}

	return matchedKeys
}

func (s *InMemoryStore) Increment(key string, by int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != StringType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	storeValue, ok := s.StringKV[key]

	if !ok {
		s.StringKV[key] = StoreValue{Value: fmt.Sprintf("%d", by)}
		s.KeyType[key] = StringType
		return by, nil
	}

	if hasExpired(storeValue.Expiry) {
		delete(s.StringKV, key)
		delete(s.KeyType, key)
		s.StringKV[key] = StoreValue{Value: fmt.Sprintf("%d", by)}
		s.KeyType[key] = StringType
		return by, nil
	}

	value, err := strconv.ParseInt(storeValue.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("-ERR value is not an integer or out of range")
	}

	if (by > 0 && value > math.MaxInt64-by) || (by < 0 && value < math.MinInt64-by) {
		return 0, fmt.Errorf("-ERR increment or decrement would overflow")
	}

	value += by
	s.StringKV[key] = StoreValue{Value: fmt.Sprintf("%d", value), Expiry: storeValue.Expiry}
	return value, nil
}

func (s *InMemoryStore) NumKeyExists(keys []string, shouldDelete bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0

	for _, k := range keys {
		if keyType, ok := s.KeyType[k]; ok {
			if shouldDelete {
				if keyType == StringType {
					delete(s.StringKV, k)
				}
				delete(s.KeyType, k)
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
			for k, v := range s.StringKV {
				if hasExpired(v.Expiry) {
					delete(s.KeyType, k)
					delete(s.StringKV, k)
				}
			}
			s.mu.Unlock()
		}
	}()
}
