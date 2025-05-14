package store

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
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

type BlockingOperationType int

const (
	BLPop BlockingOperationType = iota
	BRPop
	BLMove
)

type InMemoryStore struct {
	KeyType               map[string]StoreType
	StringKV              map[string]StoreValue
	ListKV                map[string]*list.List
	SetKV                 map[string]map[string]struct{}
	BlockedListOperations map[string][]OperationInfo
	mu                    sync.RWMutex
}

type OperationInfo struct {
	Client        chan ListElement
	OperationType BlockingOperationType

	// for BLMove
	PopFromLeft bool
}
type ListElement struct {
	key   string
	value string
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		KeyType:               make(map[string]StoreType),
		StringKV:              make(map[string]StoreValue),
		ListKV:                make(map[string]*list.List),
		SetKV:                 make(map[string]map[string]struct{}),
		BlockedListOperations: make(map[string][]OperationInfo),
	}
	s.BackgroundKeyCleanup(15000)
	return s
}

func NewList() *list.List {
	return list.New()
}

func (s *InMemoryStore) LPush(key string, values []string) error {
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

	for _, value := range values {
		s.ListKV[key].PushFront(value)
	}

	if requests, isBlocked := s.BlockedListOperations[key]; isBlocked {
		opInfo := requests[0]
		var value string

		if opInfo.OperationType == BRPop || (opInfo.OperationType == BLMove && !opInfo.PopFromLeft) {
			value = s.ListKV[key].Remove(s.ListKV[key].Back()).(string)
		} else if opInfo.OperationType == BLPop || (opInfo.OperationType == BLMove && opInfo.PopFromLeft) {
			value = s.ListKV[key].Remove(s.ListKV[key].Front()).(string)
		}

		if s.ListKV[key].Len() == 0 {
			delete(s.ListKV, key)
			delete(s.KeyType, key)
		}

		opInfo.Client <- ListElement{key: key, value: value}
		if len(requests) > 1 {
			s.BlockedListOperations[key] = requests[1:]
		} else {
			delete(s.BlockedListOperations, key)
		}
		return nil
	}

	return nil
}
func (s *InMemoryStore) RPush(key string, values []string) error {
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

	for _, value := range values {
		s.ListKV[key].PushBack(value)
	}

	if requests, isBlocked := s.BlockedListOperations[key]; isBlocked {
		opInfo := requests[0]
		var value string

		if opInfo.OperationType == BRPop || (opInfo.OperationType == BLMove && !opInfo.PopFromLeft) {
			value = s.ListKV[key].Remove(s.ListKV[key].Back()).(string)
		} else if opInfo.OperationType == BLPop || (opInfo.OperationType == BLMove && opInfo.PopFromLeft) {
			value = s.ListKV[key].Remove(s.ListKV[key].Front()).(string)
		}

		if s.ListKV[key].Len() == 0 {
			delete(s.ListKV, key)
			delete(s.KeyType, key)
		}

		opInfo.Client <- ListElement{key: key, value: value}
		if len(requests) > 1 {
			s.BlockedListOperations[key] = requests[1:]
		} else {
			delete(s.BlockedListOperations, key)
		}
		return nil
	}

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
func (s *InMemoryStore) BLPop(keys []string, timeout time.Duration, popFromRight bool) (string, string, error) {
	s.mu.Lock()

	for _, key := range keys {
		keyType, ok := s.KeyType[key]
		if ok && keyType != ListType {
			s.mu.Unlock()
			return "", "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	for _, key := range keys {
		if _, ok := s.ListKV[key]; ok {
			var value string
			if popFromRight {
				value = s.ListKV[key].Remove(s.ListKV[key].Back()).(string)
			} else {
				value = s.ListKV[key].Remove(s.ListKV[key].Front()).(string)
			}
			if s.ListKV[key].Len() == 0 {
				delete(s.ListKV, key)
				delete(s.KeyType, key)
			}
			s.mu.Unlock()
			return key, value, nil
		}
	}

	resultChan := make(chan ListElement, 1)

	cleanupFunc := func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		for _, key := range keys {
			for i, opInfo := range s.BlockedListOperations[key] {
				if opInfo.Client == resultChan {
					s.BlockedListOperations[key] = slices.Delete(s.BlockedListOperations[key], i, i+1)
					break
				}
			}

			if len(s.BlockedListOperations[key]) == 0 {
				delete(s.BlockedListOperations, key)
			}
		}
	}

	var operationType BlockingOperationType
	if popFromRight {
		operationType = BRPop
	} else {
		operationType = BLPop
	}

	operationInfo := OperationInfo{
		Client:        resultChan,
		OperationType: operationType,
	}
	for _, key := range keys {
		s.BlockedListOperations[key] = append(s.BlockedListOperations[key], operationInfo)
	}

	s.mu.Unlock()
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	} else {
		timer = make(<-chan time.Time)
	}

	select {
	case result := <-resultChan:
		cleanupFunc()
		return result.key, result.value, nil
	case <-timer:
		cleanupFunc()
		return "", "", nil
	}
}

func (s *InMemoryStore) BLMove(source string, destination string, leftSrc bool, leftDest bool, timeout time.Duration) (string, error) {
	s.mu.Lock()

	srcKeyType, ok := s.KeyType[source]
	if ok && srcKeyType != ListType {
		s.mu.Unlock()
		return "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	destKeyType, ok := s.KeyType[destination]
	if ok && destKeyType != ListType {
		s.mu.Unlock()
		return "", errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.ListKV[destination]; !ok {
		s.ListKV[destination] = NewList()
		s.KeyType[destination] = ListType
	}

	if _, ok := s.ListKV[source]; ok {
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

		s.mu.Unlock()
		return value, nil
	}

	resultChan := make(chan ListElement, 1)

	cleanupFunc := func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		for i, opInfo := range s.BlockedListOperations[source] {
			if opInfo.Client == resultChan {
				s.BlockedListOperations[source] = slices.Delete(s.BlockedListOperations[source], i, i+1)
				break
			}
		}

		if len(s.BlockedListOperations[source]) == 0 {
			delete(s.BlockedListOperations, source)
		}
	}
	operationInfo := OperationInfo{
		Client:        resultChan,
		OperationType: BLMove,
		PopFromLeft:   leftSrc,
	}
	s.BlockedListOperations[source] = append(s.BlockedListOperations[source], operationInfo)

	s.mu.Unlock()
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	} else {
		timer = make(<-chan time.Time)
	}

	select {
	case result := <-resultChan:
		cleanupFunc()

		s.mu.Lock()
		if leftDest {
			s.ListKV[destination].PushFront(result.value)
		} else {
			s.ListKV[destination].PushBack(result.value)
		}
		s.mu.Unlock()
		return result.value, nil
	case <-timer:
		cleanupFunc()
		return "", nil
	}
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

func (s *InMemoryStore) SAdd(key string, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != SetType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.SetKV[key]; !ok {
		s.SetKV[key] = make(map[string]struct{})
		s.KeyType[key] = SetType
	}

	if _, ok := s.SetKV[key][value]; !ok {
		s.SetKV[key][value] = struct{}{}
		return 1, nil
	}
	return 0, nil
}
func (s *InMemoryStore) SRem(key string, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != SetType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.SetKV[key]; !ok {
		return 0, nil
	}

	if _, ok := s.SetKV[key][value]; ok {
		delete(s.SetKV[key], value)
		if len(s.SetKV[key]) == 0 {
			delete(s.SetKV, key)
			delete(s.KeyType, key)
		}
		return 1, nil
	}
	return 0, nil
}

func (s *InMemoryStore) SIsMember(key string, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != SetType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.SetKV[key]; !ok {
		return 0, nil
	}

	if _, ok := s.SetKV[key][value]; ok {
		return 1, nil
	}
	return 0, nil
}

func (s *InMemoryStore) SInter(keys []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elementsFreq := make(map[string]int)

	for _, key := range keys {
		keyType, ok := s.KeyType[key]
		if ok && keyType != SetType {
			return nil, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if _, ok := s.SetKV[key]; !ok {
			return nil, nil
		}

		for element := range s.SetKV[key] {
			elementsFreq[element]++
		}
	}

	var result []string
	for element, freq := range elementsFreq {
		if freq == len(keys) {
			result = append(result, element)
		}
	}
	return result, nil
}

func (s *InMemoryStore) SCard(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != SetType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.SetKV[key]; !ok {
		return 0, nil
	}

	return len(s.SetKV[key]), nil
}

func (s *InMemoryStore) SMembers(key string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyType, ok := s.KeyType[key]
	if ok && keyType != SetType {
		return nil, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.SetKV[key]; !ok {
		return nil, nil
	}

	var members []string
	for member := range s.SetKV[key] {
		members = append(members, member)
	}
	return members, nil
}

func (s *InMemoryStore) SUnion(keys []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elements := make(map[string]struct{})

	for _, key := range keys {
		keyType, ok := s.KeyType[key]
		if ok && keyType != SetType {
			return nil, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if _, ok := s.SetKV[key]; !ok {
			continue
		}

		for element := range s.SetKV[key] {
			elements[element] = struct{}{}
		}
	}

	var result []string
	for element := range elements {
		result = append(result, element)
	}
	return result, nil
}

func (s *InMemoryStore) SMove(source string, destination string, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	srcType, ok := s.KeyType[source]
	if ok && srcType != SetType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	destType, ok := s.KeyType[destination]
	if ok && destType != SetType {
		return 0, errors.New("-WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if _, ok := s.SetKV[source]; !ok {
		return 0, nil
	}

	if _, ok := s.SetKV[destination]; !ok {
		s.SetKV[destination] = make(map[string]struct{})
		s.KeyType[destination] = SetType
	}

	if _, ok := s.SetKV[source][value]; ok {
		delete(s.SetKV[source], value)
		s.SetKV[destination][value] = struct{}{}
		return 1, nil
	}
	return 0, nil
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
