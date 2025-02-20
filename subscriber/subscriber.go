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

func (s *Subscriber) IsRunning() bool {
	return !errors.Is(s.closeCTX.Err(), context.Canceled)
}

func (s *Subscriber) Join() {
	<-s.closeCTX.Done()
}
