package route

import (
	"context"
	"log"
	"maps"
	"sync"
	"time"

	Messages "github.com/Ingo-Braun/TinyQ/structs/messages"
)

const maxChanSize int = 30

type Route struct {
	Channel           chan *Messages.RouterMessage
	reDeliveryChannel chan *Messages.RouterMessage
	waitingMessages   map[string]*Messages.RouterMessage
	mutex             sync.Mutex
	WaitRoutineCTX    context.Context
	WaitRoutineCancel context.CancelFunc
}

func (r *Route) GetMessage() (*Messages.RouterMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	for {

		select {
		case <-ctx.Done():
			cancel()
			return nil, false

		case message := <-r.reDeliveryChannel:
			cancel()
			message.SetDeliveryTimer()
			r.mutex.Lock()
			r.waitingMessages[message.GetId()] = message
			r.mutex.Unlock()
			return message, true
		case message := <-r.Channel:
			cancel()
			message.SetDeliveryTimer()
			r.mutex.Lock()
			r.waitingMessages[message.GetId()] = message
			r.mutex.Unlock()
			return message, true
		}
	}
}

func (r *Route) Size() int {
	return len(r.Channel) + len(r.reDeliveryChannel)
}

func (r *Route) watchTimedOutMessages() {
	for {
		select {
		case <-r.WaitRoutineCTX.Done():
			return
		default:
			r.mutex.Lock()

			for key := range maps.Keys(r.waitingMessages) {
				message, ok := r.waitingMessages[key]
				if ok && message.IsExpired() {
					log.Printf("message %v from route %v has expired ", message.GetId(), message.Route)
					r.reDeliveryChannel <- message
					delete(r.waitingMessages, key)
				}
			}
			r.mutex.Unlock()
			time.Sleep(time.Millisecond * 10)
		}

	}
}

func SetupRoute() (*Route, chan *Messages.RouterMessage) {
	outputChannel := make(chan *Messages.RouterMessage, maxChanSize)
	route := Route{
		Channel:           outputChannel,
		reDeliveryChannel: make(chan *Messages.RouterMessage, maxChanSize),
		waitingMessages:   make(map[string]*Messages.RouterMessage),
	}
	route.WaitRoutineCTX, route.WaitRoutineCancel = context.WithCancel(context.Background())
	go route.watchTimedOutMessages()
	return &route, outputChannel
}
