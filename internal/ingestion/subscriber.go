package ingestion

import (
	"sync"

	"GoTTDust/internal/common"
)

// Subscriber receives live records for a stream.
type Subscriber struct {
	streamID common.StreamID
	ch       chan *common.ValidatedRecord
	closed   bool
	mu       sync.Mutex
}

// NewSubscriber creates a new subscriber for a stream.
func NewSubscriber(streamID common.StreamID, bufferSize int) *Subscriber {
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	return &Subscriber{
		streamID: streamID,
		ch:       make(chan *common.ValidatedRecord, bufferSize),
	}
}

// Ch returns the channel to receive records from.
func (s *Subscriber) Ch() <-chan *common.ValidatedRecord {
	return s.ch
}

// Send sends a record to the subscriber. Returns false if closed or full.
func (s *Subscriber) Send(record *common.ValidatedRecord) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()

	select {
	case s.ch <- record:
		return true
	default:
		return false // Channel full, drop record
	}
}

// Close closes the subscriber channel.
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}

// SubscriptionManager manages live-tail subscribers per stream.
type SubscriptionManager struct {
	mu          sync.RWMutex
	subscribers map[common.StreamID][]*Subscriber
}

// NewSubscriptionManager creates a new subscription manager.
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		subscribers: make(map[common.StreamID][]*Subscriber),
	}
}

// Subscribe creates a new subscriber for a stream.
func (sm *SubscriptionManager) Subscribe(streamID common.StreamID) *Subscriber {
	sub := NewSubscriber(streamID, 1000)

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.subscribers[streamID] = append(sm.subscribers[streamID], sub)

	return sub
}

// Unsubscribe removes a subscriber.
func (sm *SubscriptionManager) Unsubscribe(sub *Subscriber) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	subs := sm.subscribers[sub.streamID]
	for i, s := range subs {
		if s == sub {
			sm.subscribers[sub.streamID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	sub.Close()
}

// Publish sends a record to all subscribers for a stream.
func (sm *SubscriptionManager) Publish(streamID common.StreamID, record *common.ValidatedRecord) {
	sm.mu.RLock()
	subs := sm.subscribers[streamID]
	sm.mu.RUnlock()

	for _, sub := range subs {
		sub.Send(record)
	}
}

// SubscriberCount returns the number of active subscribers for a stream.
func (sm *SubscriptionManager) SubscriberCount(streamID common.StreamID) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.subscribers[streamID])
}
