package transport

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/mark3labs/mcp-go/mcp"
)

// Base contains common fields and methods shared across transport implementations.
type Base struct {
	// Handler for JSON-RPC notifications
	notificationHandler func(mcp.JSONRPCNotification)
	notifyMu           sync.RWMutex

	// Map of request IDs to response channels
	responses map[int64]chan *JSONRPCResponse
	responsesOnce sync.Once
	mu        sync.RWMutex

	// State tracking
	started atomic.Bool
	closed  atomic.Bool
}

// SetNotificationHandler sets the handler for notifications.
func (b *Base) SetNotificationHandler(handler func(notification mcp.JSONRPCNotification)) {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	b.notificationHandler = handler
}

// HandleNotification calls the notification handler if set.
func (b *Base) HandleNotification(notification mcp.JSONRPCNotification) {
	b.notifyMu.RLock()
	defer b.notifyMu.RUnlock()
	if b.notificationHandler != nil {
		b.notificationHandler(notification)
	}
}

// initResponses lazily initializes the responses map if needed
func (b *Base) initResponses() {
	b.responsesOnce.Do(func() {
		b.responses = make(map[int64]chan *JSONRPCResponse)
	})
}

// NewResponse creates a new response channel for the given request ID.
func (b *Base) NewResponse(id int64) chan *JSONRPCResponse {
	b.initResponses()
	responseChan := make(chan *JSONRPCResponse, 1)
	b.mu.Lock()
	b.responses[id] = responseChan
	b.mu.Unlock()
	return responseChan
}

// SendResponse delivers the response to the waiting channel and removes it from the map.
func (b *Base) SendResponse(id int64, response *JSONRPCResponse) bool {
	b.initResponses()
	b.mu.RLock()
	ch, ok := b.responses[id]
	b.mu.RUnlock()

	if ok {
		ch <- response
		b.mu.Lock()
		delete(b.responses, id)
		b.mu.Unlock()
		return true
	}
	return false
}

// Close closes all response channels and clears the map.
func (b *Base) Close() {
	if b.responses != nil {
		b.mu.Lock()
		for _, ch := range b.responses {
			close(ch)
		}
		b.responses = nil
		b.mu.Unlock()
	}

	b.closed.Store(true)
}

// RemoveResponse removes a response channel from the map without sending a response.
func (b *Base) RemoveResponse(id int64) {
	b.initResponses()
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.responses, id)
}

// IsStarted returns whether the transport has been started.
func (b *Base) IsStarted() bool {
	return b.started.Load()
}

// Start sets the started state to true.
func (b *Base) Start(_ context.Context) {
	b.started.Store(true)
}

// IsClosed returns whether the transport has been closed.
func (b *Base) IsClosed() bool {
	return b.closed.Load()
}

// AlreadyClosed atomically sets closed to true if it was false.
// Returns true if the transport was already closed.
func (b *Base) AlreadyClosed() bool {
	return !b.closed.CompareAndSwap(false, true)
}
