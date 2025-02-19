package route

import (
	"context"
	"log"
	"maps"
	"sync"
	"time"

	Messages "github.com/Ingo-Braun/TinyQ/structs/messages"
)

const defaultMaxChanSize int = 30

type MessageStorage struct {
	ConsumerId string
	Message    *Messages.RouterMessage
}

type Route struct {
	Channel           chan *Messages.RouterMessage
	reDeliveryChannel chan *Messages.RouterMessage
	waitingMessages   map[string]MessageStorage
	mutex             sync.Mutex
	WaitRoutineCTX    context.Context
	WaitRoutineCancel context.CancelFunc
}

func (r *Route) GetMessage(consumerId string) (*Messages.RouterMessage, bool) {
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
			r.waitingMessages[message.GetId()] = MessageStorage{
				ConsumerId: consumerId,
				Message:    message,
			}
			r.mutex.Unlock()
			return message, true
		case message := <-r.Channel:
			cancel()
			message.SetDeliveryTimer()
			r.mutex.Lock()
			r.waitingMessages[message.GetId()] = MessageStorage{
				ConsumerId: consumerId,
				Message:    message,
			}
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
				messageStorage, ok := r.waitingMessages[key]
				if ok && messageStorage.Message.IsExpired() {
					log.Printf("message %v from route %v has expired ", messageStorage.Message.GetId(), messageStorage.Message.Route)
					r.reDeliveryChannel <- messageStorage.Message
					delete(r.waitingMessages, key)
				}
			}
			r.mutex.Unlock()
			time.Sleep(time.Millisecond * 10)
		}

	}
}

func (r *Route) Ack(consumerId string, messageId string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	messageStorage, ok := r.waitingMessages[messageId]
	if ok && messageStorage.ConsumerId == consumerId {
		messageStorage.Message.Ack()
		return true
	}
	return false
}
func (r *Route) GetConsummerId(message *Messages.RouterMessage) (string, bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	messageStorage, ok := r.waitingMessages[message.GetId()]
	if ok {
		return messageStorage.ConsumerId, true
	}
	return "", false
}

func SetupRoute() (*Route, chan *Messages.RouterMessage) {
	outputChannel := make(chan *Messages.RouterMessage, defaultMaxChanSize)
	route := Route{
		Channel:           outputChannel,
		reDeliveryChannel: make(chan *Messages.RouterMessage, defaultMaxChanSize),
		waitingMessages:   make(map[string]MessageStorage),
	}
	route.WaitRoutineCTX, route.WaitRoutineCancel = context.WithCancel(context.Background())
	go route.watchTimedOutMessages()
	return &route, outputChannel
}
