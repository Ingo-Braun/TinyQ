package messages

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

const DeliveryTimeout = 200

type RouterMessage struct {
	id            string
	Route         string
	Content       []byte
	RetrySend     int
	deliverCTX    context.Context
	deliverCancel context.CancelFunc
}

func (r *RouterMessage) GetId() string {
	return r.id
}

func (r *RouterMessage) Ack() bool {
	r.deliverCancel()
	return r.deliverCTX.Err() == context.Canceled
}

func (r *RouterMessage) IsValid() bool {
	return (r.deliverCTX.Err() == nil || errors.Is(r.deliverCTX.Err(), context.Canceled))
}

func (r *RouterMessage) IsExpired() bool {
	return errors.Is(r.deliverCTX.Err(), context.DeadlineExceeded)
}

func (r *RouterMessage) DeliveryErr() error {
	return r.deliverCTX.Err()
}

func (r *RouterMessage) SetDeliveryTimer() {
	r.deliverCTX, r.deliverCancel = context.WithTimeout(context.Background(), time.Millisecond*DeliveryTimeout)
	r.RetrySend++
}

func CreateMessage(content []byte, routeKey string) *RouterMessage {
	return &RouterMessage{
		Route:     routeKey,
		Content:   content,
		RetrySend: 0,
		id:        uuid.New().String(),
	}
}
