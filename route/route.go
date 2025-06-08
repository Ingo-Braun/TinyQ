package route

import (
	"context"
	"errors"
	"log"
	"maps"
	"sync"
	"time"

	"github.com/Ingo-Braun/TinyQ/hooks"
	Messages "github.com/Ingo-Braun/TinyQ/messages"
)

const MessageRetrievalTimeout = 100

// message storage to awaiting confirmation (Ack) messages
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
	// map of messages awaiting confirmation [messageId]messageStorage struct
	awaitingMessages map[string]MessageStorage
	// waiting messages mutex
	awaitingMessagesMutex sync.Mutex
	// context to control expired messages routine
	WaitRoutineCTX    context.Context
	WaitRoutineCancel context.CancelFunc
	// context to control when the route is closed
	CloseCTX    context.Context
	CloseCancel context.CancelFunc
	// router closing context
	RouterCloseCTX context.Context
	// Size of the channel
	ChanSize int

	hookExecutor     *hooks.HookExecutor
	hookInputChannel chan *Messages.RouterMessage
	hooksEnabled     bool
	hooksEnableMutex sync.Mutex
}

// validate and propagates the closing context of the Router
func (r *Route) checkIfRouterIsClosed() {
	select {
	case <-r.RouterCloseCTX.Done():
		r.CloseCancel()
		r.WaitRoutineCancel()
		if r.hooksEnabled {
			r.hookExecutor.Stop()
		}
	default:
		return
	}
}

// Attempts to retrieve an message in MessageRetrievalTimeout milliseconds (ms)
// Return nil,false if the Route is Closed
// Return nil,false if message retrieval failed
// Attempts to retrieve from priority channel (reDeliveryChannel)
// If fails to get from reDeliveryChannel
// Attempts to retrieve from priority channel (reDeliveryChannel)
// On retrieval success lock the awaiting messages map
// Set the message timer
// Creates an message container with the consumer id and the message
// Puts the message in the awaiting messages map
// Unlocks the awaiting messages map
// Returns message,true
func (r *Route) GetMessage(consumerId string) (*Messages.RouterMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*MessageRetrievalTimeout)
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
// since both channels are live when measuring it is not ensure of an accurate number
// NOT ACCURATE
func (r *Route) Size() int {
	r.checkIfRouterIsClosed()
	return len(r.Channel) + len(r.reDeliveryChannel)
}

// Routine that checks on the awaiting messages map in search of expired messages
// upon finding an expired message removes from the awaiting messages map and puts on the priority queue
func (r *Route) watchTimedOutMessages() {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		select {
		case <-r.WaitRoutineCTX.Done():
			return
		case <-ticker.C:
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
		}

	}
}

// Confirms an message
// Messages can only be confirmed by the consumerId in the message container in the awaiting message map [messageId].ConsumerId
// This prevents an race condition where an late consumer
// that acquired first an message can confirm a message that has been delivered to other consumer
func (r *Route) Ack(consumerId string, messageId string) bool {
	r.awaitingMessagesMutex.Lock()
	defer r.awaitingMessagesMutex.Unlock()
	messageStorage, ok := r.awaitingMessages[messageId]
	if ok && messageStorage.ConsumerId == consumerId {
		messageStorage.Message.Ack()
		r.hooksEnableMutex.Lock()
		defer r.hooksEnableMutex.Unlock()
		if r.hooksEnabled {
			// make an copy of the message data
			messageCopy := &messageStorage.Message
			r.hookInputChannel <- *messageCopy
		}
		delete(r.awaitingMessages, messageId)
		return true
	}
	return false
}

// Returns the Consumer id of an awaiting message map
// This can be used to check if an consumer still is responsible for that message
// Changes when an expired message get processed by the expired messages routine
// Changes when an message is retrieved by an consumer
func (r *Route) GetConsumerId(message *Messages.RouterMessage) (string, bool) {
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
		hooksEnabled:      false,
	}
	route.WaitRoutineCTX, route.WaitRoutineCancel = context.WithCancel(context.Background())
	go route.watchTimedOutMessages()
	return &route, outputChannel
}

// Closes the route
// This propagates to all consumers linked to this route
// WARNING calling this will delete all messages in the route
func (r *Route) CloseRoute() {
	if !r.IsClosed() {
		r.CloseCancel()
		r.WaitRoutineCancel()
	}
}

func (r *Route) IsClosed() bool {
	return errors.Is(r.CloseCTX.Err(), context.Canceled) || errors.Is(r.RouterCloseCTX.Err(), context.Canceled)
}

// Validates if an message is in awaiting ack
// returns true if message is in awaiting ack
func (r *Route) IsWaitingAck(messageId string) bool {
	r.awaitingMessagesMutex.Lock()
	defer r.awaitingMessagesMutex.Unlock()
	_, ok := r.awaitingMessages[messageId]
	return ok
}

func (r *Route) IsHooksEnabled() bool {
	r.hooksEnableMutex.Lock()
	defer r.hooksEnableMutex.Unlock()
	return r.hooksEnabled
}

// add an post acknowledge hook to execute
// returns an error from hooks if fails
// see hooks HooksExecutor.AddHook for an complete list
func (r *Route) AddHook(hook hooks.Hook) error {
	r.EnableHooks()
	err := r.hookExecutor.AddHook(hook)
	return err
}

func (r *Route) EnableHooks() {
	r.hooksEnableMutex.Lock()
	if !r.hooksEnabled {
		r.hooksEnabled = true
		r.hookExecutor = hooks.CreateHookExecutor(true)
		r.hookExecutor.StartExecutor()
		r.hookInputChannel = r.hookExecutor.GetInputChannel()
	}
	r.hooksEnableMutex.Unlock()
}

// return the number of registered hooks
func (r *Route) HooksCount() int {
	if r.hooksEnabled {
		return r.hookExecutor.Count()
	}
	return -1
}
