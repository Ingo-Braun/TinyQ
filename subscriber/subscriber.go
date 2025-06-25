package subscriber

import (
	"context"
	"log"
	"time"

	"errors"

	Messages "github.com/Ingo-Braun/TinyQ/messages"
	Route "github.com/Ingo-Braun/TinyQ/route"
	"github.com/google/uuid"
)

const consumerPullDelay = 100

type Subscriber struct {
	id            string
	route         *Route.Route
	routeCloseCTX context.Context
	callBackFunc  CallBack
	closeCTX      context.Context
	closeCancel   context.CancelFunc
}

type CallBack func(*Messages.RouterMessage, context.CancelFunc)

func (s *Subscriber) Setup(route *Route.Route, callbackFunc CallBack) {
	s.id = uuid.New().String()
	s.route = route
	s.routeCloseCTX = route.CloseCTX
	s.closeCTX, s.closeCancel = context.WithCancel(context.Background())
	s.callBackFunc = callbackFunc
	go s.Consumer()
}

func (s *Subscriber) Close() {
	s.closeCancel()
}

func (s *Subscriber) Consumer() {
	for {
		select {
		case <-s.routeCloseCTX.Done():
			return
		case <-s.closeCTX.Done():
			return
		default:
			message, ok := s.route.GetMessage(s.id)
			if ok {
				ctx, confirm := context.WithCancel(context.Background())
				s.callBackFunc(message, confirm)
				if errors.Is(ctx.Err(), context.Canceled) {
					ok = s.route.Ack(s.id, message.GetId())
					if !ok {
						log.Printf("subscriber %v failed to confirm message %v\n", s.id, message.GetId())
					}
				}
			} else {
				time.Sleep(time.Millisecond * consumerPullDelay)
			}
		}
	}
}

// joins the subscriber close context and route close context
// returns if the route context OR the subscriber context is closed
func (s *Subscriber) Join() {
	for {
		select {
		case <-s.closeCTX.Done():
			return
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// check if the route is closed or its own close context has been called
// if the route is not active than there is no reason this should be active
func (s *Subscriber) IsRunning() bool {
	return !errors.Is(s.closeCTX.Err(), context.Canceled) || !errors.Is(s.routeCloseCTX.Err(), context.Canceled)
}

// check if the route is closed or its own close context has been called
// if the route is not active than there is no reason this should be active
// this is to keep consistency to other consumers
func (s *Subscriber) IsClosed() bool {
	return errors.Is(s.closeCTX.Err(), context.Canceled) || errors.Is(s.routeCloseCTX.Err(), context.Canceled)
}
