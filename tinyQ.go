package tinyQ

import (
	"context"
	"log"
	"sync"
	"time"

	"errors"

	"github.com/Ingo-Braun/TinyQ/consumer"
	Messages "github.com/Ingo-Braun/TinyQ/messages"
	"github.com/Ingo-Braun/TinyQ/publishers/simple"
	Route "github.com/Ingo-Braun/TinyQ/route"
	Subscriber "github.com/Ingo-Braun/TinyQ/subscriber"
)

const version = "v0.2.2-alpha-1"

// N times witch the router will try to deliver
// TODO: allow retry count as an configurable varibale
const reDeliverCount int = 5

// Max messages in queue to an route until block
// TODO: allow max queue size as an configurable variable on route creation
const maxChanSize int = 30

// Main router, responsible to route messages into routes
// call InitRouter to initialize the router
// call StopRouter to stop the routing process WARNING this kills all routes and deletes all messages on the routes
type Router struct {
	// Routing map used to store all known routes [routeKey]*Route
	Routes map[string]*Route.Route
	// Routing map mutex
	routesMutex sync.Mutex
	// Router input channel - where the messages come
	RouterInput chan *Messages.RouterMessage
	// Router re-send channel not used
	ResendChannel chan *Messages.RouterMessage
	// Router stop context
	stopCTX       context.Context
	stopCTXCancel context.CancelFunc
}

// Ad-hoc message deliver delivery`s a message widouth the need to use an publisher
func (router *Router) deliverMessage(routerMessage *Messages.RouterMessage, destinationRoute *Route.Route) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
writeLoop:
	for {
		select {
		case <-ctx.Done():
			routerMessage.RetrySend++
			// router.ResendChannel <- routerMessage
			router.RouterInput <- routerMessage
			break writeLoop
		default:

			if len(destinationRoute.Channel) < maxChanSize {
				destinationRoute.Channel <- routerMessage
				break writeLoop
			}
		}
	}
}

// Router worker to pass messages from the input channel to destination routes
// messages with an invalid destination are discarted
// this worker temporary locks the Routes map to deliver the message
// this will try to deliver the message up to reDeliverCount (5)
func (router *Router) routerDistributionWorker(cancelCTX context.Context) {
	var routerMessage *Messages.RouterMessage
	for {
		select {
		case <-cancelCTX.Done():
			log.Println("stopping router consumer")
			return
		case routerMessage = <-router.RouterInput:
			if routerMessage.RetrySend < reDeliverCount {
				destinationRoute, ok := router.GetRoute(routerMessage.Route)
				if ok {
					router.deliverMessage(routerMessage, destinationRoute)
					continue
				}
				log.Printf("discarting message to route %v due not being registred\n", routerMessage.Route)
			}
			log.Println("discarting message due to max retries exceded")
		}
	}
}

// Initializes the router and start the router distibution worker
// Call this BEFORE using anything from the router
// NIL pointer exceptions will be rised if not called before use
func (router *Router) InitRouter() {
	log.Println("starting router")
	router.RouterInput = make(chan *Messages.RouterMessage)
	router.Routes = make(map[string]*Route.Route)
	router.stopCTX, router.stopCTXCancel = context.WithCancel(context.Background())
	go router.routerDistributionWorker(router.stopCTX)
	log.Println("router started")
}

// Stops everything
// distribution worker will be stopped
// publishers will be stopped
// consumers will be stopped
func (router *Router) StopRouter() {
	router.stopCTXCancel()
}

// Register a new route
// Call this BEFORE publishing any message
// Calling two times on same route key is fine
func (router *Router) RegisterRoute(routeKey string) {
	if !router.HasRoute(routeKey) {
		router.routesMutex.Lock()
		route, _ := Route.SetupRoute(router.stopCTX)
		router.Routes[routeKey] = route
		router.routesMutex.Unlock()
	}
}

// Removes an route
// This will stop all consumers connected to this route and deletes all messages
// Calling twice on same route key is fine
func (router *Router) UnregisterRoute(routeKey string) {
	router.routesMutex.Lock()
	delete(router.Routes, routeKey)
	router.routesMutex.Unlock()
}

// Returns the router input channel as an ponter
// you should never need this use with caution
// Warning closing this channel will break things widout any chance of recover
func (router *Router) GetInputChannel() *chan *Messages.RouterMessage {
	return &router.RouterInput
}

// Creates and returns a new consumer to an route key
// Every consumer is thread safe
// use as many as you need
func (router *Router) GetConsumer(routeKey string) *consumer.Consumer {
	consumer := consumer.Consumer{}
	if !router.HasRoute(routeKey) {
		router.RegisterRoute(routeKey)
	}
	route, ok := router.GetRoute(routeKey)
	if ok {
		consumer.Setup(route)
		return &consumer
	}
	return nil
}

// Return an pinter to the Route object used by the router to receive messages from the main input
// Use with caution
func (router *Router) GetRoute(routeKey string) (*Route.Route, bool) {
	router.routesMutex.Lock()
	route, ok := router.Routes[routeKey]
	router.routesMutex.Unlock()
	return route, ok
}

// Creates and return a new publisher vinculated to this router
// Every message published goes to the router input channel for distribution
// Every publisher is thread safe
// Use as many as you need
func (router *Router) GetPublisher() *simple.SimplePublisher {
	publisher := simple.SimplePublisher{}
	publisher.StartPublisher(router.GetInputChannel(), router.stopCTX)
	return &publisher
}

// Checks if the router has an route with that route key
// This locks the Routes map to get the awnser
func (router *Router) HasRoute(routeKey string) bool {
	router.routesMutex.Lock()
	_, ok := router.Routes[routeKey]
	router.routesMutex.Unlock()
	return ok
}

// Checks if the router is running
func (router *Router) IsRunning() bool {
	return !errors.Is(router.stopCTX.Err(), context.Canceled)
}

func (router *Router) GetSubscriber(routeKey string, callBack Subscriber.CallBack) (*Subscriber.Subscriber, bool) {
	if router.HasRoute(routeKey) {
		subscriber := Subscriber.Subscriber{}
		route, ok := router.GetRoute(routeKey)
		if !ok {
			return nil, false
		}
		subscriber.Setup(route, callBack)
		return &subscriber, true
	}
	return nil, false
}
