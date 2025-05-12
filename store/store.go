package store

import (
	"container/list"
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
	ListKV   map[string]*list.List
	mu       sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		KeyType:  make(map[string]StoreType),
		StringKV: make(map[string]StoreValue),
		ListKV:   make(map[string]*list.List),
	}
	s.BackgroundKeyCleanup(15000)
	return s
}

func NewList() *list.List {
	return list.New()
}

func (s *InMemoryStore) LPush(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.ListKV[key]; !ok {
		s.ListKV[key] = NewList()
		s.KeyType[key] = ListType
	}

	s.ListKV[key].PushFront(value)
	return nil
}
func (s *InMemoryStore) RPush(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := s.ListKV[key]; !ok {
		s.ListKV[key] = NewList()
		s.KeyType[key] = ListType
	}
	s.ListKV[key].PushBack(value)
	return nil
}
func (s *InMemoryStore) LPop(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := s.ListKV[key]; !ok {
		return "", errors.New("_")
	}

	value := s.ListKV[key].Remove(s.ListKV[key].Front())
	if s.ListKV[key].Len() == 0 {
		delete(s.ListKV, key)
		delete(s.KeyType, key)
	}
	return value.(string), nil
}
func (s *InMemoryStore) RPop(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := s.ListKV[key]; !ok {
		return "", errors.New("_")
	}
	value := s.ListKV[key].Remove(s.ListKV[key].Back())
	if s.ListKV[key].Len() == 0 {
		delete(s.ListKV, key)
		delete(s.KeyType, key)
	}
	return value.(string), nil
}
func (s *InMemoryStore) LLen(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := s.ListKV[key]; !ok {
		return 0, nil
	}
	return s.ListKV[key].Len(), nil
}

func (s *InMemoryStore) LRange(key string, start int, end int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return nil, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := s.ListKV[key]; !ok {
		return nil, nil
	}

	listLen := s.ListKV[key].Len()
	if start >= listLen || start > end {
		return nil, nil
	}

	if start < 0 {
		start = listLen + start
	}
	if end < 0 {
		end = listLen + end
	}
	if end >= listLen {
		end = listLen - 1
	}

	var values []string
	curElement := s.ListKV[key].Front()
	curIndex := 0

	for curElement != nil && curIndex < start {
		curElement = curElement.Next()
		curIndex++
	}

	for curElement != nil && curIndex <= end {
		values = append(values, curElement.Value.(string))
		curElement = curElement.Next()
		curIndex++
	}

	return values, nil
}

func (s *InMemoryStore) LTrim(key string, start int, end int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	keyType, ok := s.KeyType[key]
	if ok && keyType != ListType {
		return errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := s.ListKV[key]; !ok {
		return nil
	}
	listLen := s.ListKV[key].Len()
	if start >= listLen || start > end {
		delete(s.ListKV, key)
		delete(s.KeyType, key)
		return nil
	}

	if start < 0 {
		start = listLen + start
	}
	if end < 0 {
		end = listLen + end
	}

	if end >= listLen {
		end = listLen - 1
	}
	for range start {
		s.ListKV[key].Remove(s.ListKV[key].Front())
	}

	for i := end + 1; i < listLen; i++ {
		s.ListKV[key].Remove(s.ListKV[key].Back())
	}
	if s.ListKV[key].Len() == 0 {
		delete(s.ListKV, key)
		delete(s.KeyType, key)
	}

	return nil
}

func (s *InMemoryStore) LMove(source string, destination string, leftSrc bool, leftDest bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	srcType, ok := s.KeyType[source]
	if ok && srcType != ListType {
		return "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	destType, ok := s.KeyType[destination]
	if ok && destType != ListType {
		return "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.ListKV[source]; !ok {
		return "", errors.New("_")
	}
	if _, ok := s.ListKV[destination]; !ok {
		s.ListKV[destination] = NewList()
		s.KeyType[destination] = ListType
	}

	var value string
	if leftSrc {
		value = s.ListKV[source].Remove(s.ListKV[source].Front()).(string)
	} else {
		value = s.ListKV[source].Remove(s.ListKV[source].Back()).(string)
	}

	if s.ListKV[source].Len() == 0 {
		delete(s.ListKV, source)
		delete(s.KeyType, source)
	}

	if leftDest {
		s.ListKV[destination].PushFront(value)
	} else {
		s.ListKV[destination].PushBack(value)
	}

	return value, nil
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
				} else if keyType == ListType {
					delete(s.ListKV, k)
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
