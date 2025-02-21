package route

import (
	"context"
	"log"
	"maps"
	"sync"
	"time"

	Messages "github.com/Ingo-Braun/TinyQ/messages"
)

const MessageRetrivalTimeout = 100

// message storage to awating confirmation (Ack) messages
type MessageStorage struct {
	ConsumerId string
	Message    *Messages.RouterMessage
}

// Route struct
// Use SetupRoute to create an new Route
// DO NOT CREATE AN ROUTE FROM SCRATCH
type Route struct {
	// channel that will hold the messages to deliver
	Channel chan *Messages.RouterMessage
	// priority channel to deliver expired messages as "new"
	reDeliveryChannel chan *Messages.RouterMessage
	// map of messages awating confirmation [messageId]messageStorage struct
	awaitingMessages map[string]MessageStorage
	// wating messages mutex
	awaitingMessagesMutex sync.Mutex
	// context to controll expired messages routine
	WaitRoutineCTX    context.Context
	WaitRoutineCancel context.CancelFunc
	// contex to controll when the route is closed
	CloseCTX    context.Context
	CloseCancel context.CancelFunc
	// router closing context
	RouterCloseCTX context.Context
	// Size of the channel
	ChanSize int
}

// validate and propagetes the closing context of the Router
func (r *Route) checkIfRouterIsClosed() {
	select {
	case <-r.RouterCloseCTX.Done():
		r.CloseCancel()
		r.WaitRoutineCancel()
	default:
		return
	}
}

// Attempts to retrive an message in MessageRetrivalTimeout milliseconds (ms)
// Return nil,false if the Route is Closed
// Return nil,false if message retrival failed
// Attempts to retrive from priority channel (reDeliveryChannel)
// If fails to get from reDeliveryChannel
// Attempts to retrive from priority channel (reDeliveryChannel)
// On retrival sucess lock the awating messages map
// Set the message timer
// Creates an message container with the consumer id and the message
// Puts the message in the awating messages map
// Unlocks the awating messages map
// Returns message,true
func (r *Route) GetMessage(consumerId string) (*Messages.RouterMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*MessageRetrivalTimeout)
	for {

		select {
		case <-ctx.Done():
			cancel()
			return nil, false

		case message := <-r.reDeliveryChannel:
			cancel()
			message.SetDeliveryTimer()
			r.awaitingMessagesMutex.Lock()
			r.awaitingMessages[message.GetId()] = MessageStorage{
				ConsumerId: consumerId,
				Message:    message,
			}
			r.awaitingMessagesMutex.Unlock()
			return message, true
		case message := <-r.Channel:
			cancel()
			message.SetDeliveryTimer()
			r.awaitingMessagesMutex.Lock()
			r.awaitingMessages[message.GetId()] = MessageStorage{
				ConsumerId: consumerId,
				Message:    message,
			}
			r.awaitingMessagesMutex.Unlock()
			return message, true
		}
	}
}

// Returns the size of the normal channel plus the priority channel
// since both channels are live when mesuring it is not garante of an acurate number
// NOT ACURATE
func (r *Route) Size() int {
	r.checkIfRouterIsClosed()
	return len(r.Channel) + len(r.reDeliveryChannel)
}

// Routine that checks on the awating messages map in search of expired messages
// upon finding an expired message removes from the awating messages map and puts on the priority queue
func (r *Route) watchTimedOutMessages() {
	for {
		select {
		case <-r.WaitRoutineCTX.Done():
			return
		default:
			r.checkIfRouterIsClosed()
			r.awaitingMessagesMutex.Lock()
			for key := range maps.Keys(r.awaitingMessages) {
				messageStorage, ok := r.awaitingMessages[key]
				if ok && messageStorage.Message.IsExpired() {
					log.Printf("message %v from route %v has expired ", messageStorage.Message.GetId(), messageStorage.Message.Route)
					r.reDeliveryChannel <- messageStorage.Message
					delete(r.awaitingMessages, key)
				}
			}
			r.awaitingMessagesMutex.Unlock()
			time.Sleep(time.Millisecond * 10)
		}

	}
}

// Confirms an message
// Messages can only be confirmed by the consumerId in the message container in the awating message map [messageId].ConsumerId
// This prevents an race condition where an late consumer
// that aquired first an message can confirm a message that has been delivered to other consumer
func (r *Route) Ack(consumerId string, messageId string) bool {
	r.awaitingMessagesMutex.Lock()
	defer r.awaitingMessagesMutex.Unlock()
	messageStorage, ok := r.awaitingMessages[messageId]
	if ok && messageStorage.ConsumerId == consumerId {
		messageStorage.Message.Ack()
		return true
	}
	return false
}

// Returns the Consumer id of an awating message map
// This can be used to check if an consumer stil is responsible for that message
// Changes when an expired message get processed by the expired messages routine
// Changes when an message is retrived by an consumer
func (r *Route) GetConsummerId(message *Messages.RouterMessage) (string, bool) {
	r.awaitingMessagesMutex.Lock()
	defer r.awaitingMessagesMutex.Unlock()
	messageStorage, ok := r.awaitingMessages[message.GetId()]
	if ok {
		return messageStorage.ConsumerId, true
	}
	return "", false
}

// Setups the Route and start the expired messages routine
func SetupRoute(routerCloseCTX context.Context, channelSize int) (*Route, chan *Messages.RouterMessage) {
	outputChannel := make(chan *Messages.RouterMessage, channelSize)
	CloseCTX, closeCancel := context.WithCancel(context.Background())
	route := Route{
		Channel:           outputChannel,
		reDeliveryChannel: make(chan *Messages.RouterMessage, channelSize),
		awaitingMessages:  make(map[string]MessageStorage),
		CloseCTX:          CloseCTX,
		CloseCancel:       closeCancel,
		RouterCloseCTX:    routerCloseCTX,
		ChanSize:          channelSize,
	}
	route.WaitRoutineCTX, route.WaitRoutineCancel = context.WithCancel(context.Background())
	go route.watchTimedOutMessages()
	return &route, outputChannel
}

// Closes the route
// This propagates to all consumers linked to this route
// WARNING calling this will delere all messages in the route
func (r *Route) CloseRoute() {
	r.CloseCancel()
	r.WaitRoutineCancel()
}
