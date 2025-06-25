package messages

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Milliseconds (ms) until message expires if not ack
const DeliveryTimeout = time.Second * 20

type RouterMessage struct {
	// Message UUID
	id string
	// Message route key
	Route string
	// Message Content
	Content []byte
	// Number of times the router have tried to deliver to an route
	RetrySend int
	// Ack timer
	deliverCTX    context.Context
	deliverCancel context.CancelFunc
}

// Returns the message UUID
func (r *RouterMessage) GetId() string {
	return r.id
}

// DO NOT CALL THIS outside the route scope
// Stops the delivery timer
func (r *RouterMessage) Ack() bool {
	r.deliverCancel()
	return errors.Is(r.deliverCTX.Err(), context.Canceled)
}

// Returns if an message is valid
// Uses the delivery timer context to evaluate if the timer is running or has been canceled
func (r *RouterMessage) IsValid() bool {
	return (r.deliverCTX.Err() == nil || errors.Is(r.deliverCTX.Err(), context.Canceled))
}

// Returns if the message delivery times has exceeded
// NOT FULL RELIABLE
// TODO: transfer check to an more reliable source
func (r *RouterMessage) IsExpired() bool {
	return errors.Is(r.deliverCTX.Err(), context.DeadlineExceeded)
}

// Returns the err of the delivery timer context
func (r *RouterMessage) DeliveryErr() error {
	return r.deliverCTX.Err()
}

// Resets the delivery timer
func (r *RouterMessage) SetDeliveryTimer() {
	r.deliverCTX, r.deliverCancel = context.WithTimeout(context.Background(), DeliveryTimeout)
	r.RetrySend++
}

// creates an new message
// Use this to create an new message outside the publishers context
func CreateMessage(content []byte, routeKey string) *RouterMessage {
	return &RouterMessage{
		Route:     routeKey,
		Content:   content,
		RetrySend: 0,
		id:        uuid.New().String(),
	}
}

type TelemetryPackage struct {
	Type  string
	Value int
}

const TelemetryTypeMessagesSent string = "Sent"
const TelemetryTypeMessagesProcessed string = "Processed"
const TelemetryTypeMessagesDiscarded string = "Discarded"
const TelemetryTypeMessagesResent string = "Resent"
