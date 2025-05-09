package tinyQ

import (
	"context"
	"log"
	"sync"
	"time"

	"errors"

	"github.com/Ingo-Braun/TinyQ/consumer"
	Messages "github.com/Ingo-Braun/TinyQ/messages"
	DedicatedPublisher "github.com/Ingo-Braun/TinyQ/publishers/dedicated"
	SimplePublisher "github.com/Ingo-Braun/TinyQ/publishers/simple"
	Route "github.com/Ingo-Braun/TinyQ/route"
	Subscriber "github.com/Ingo-Braun/TinyQ/subscriber"
)

const Version = "v0.9.1"

// N times witch the router will try to deliver
// TODO: allow retry count as an configurable variable
const reDeliverCount int = 5

// default maximum of messages in an Route
const DefaultMaxRouteSize int = 300

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
	stopCTX          context.Context
	stopCTXCancel    context.CancelFunc
	odometer         bool
	telemetryMutex   sync.Mutex
	telemetry        *Telemetry
	telemetryChannel chan Messages.TelemetryPackage
}

// Ad-hoc message deliver delivery`s a message widout the need to use an publisher
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

			if len(destinationRoute.Channel) < destinationRoute.ChanSize {
				destinationRoute.Channel <- routerMessage
				if router.odometer {
					router.telemetryChannel <- Messages.TelemetryPackage{
						Type:  Messages.TelemetryTypeMessagesProcessed,
						Value: 1,
					}
				}
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
			if router.odometer {
				router.telemetryChannel <- Messages.TelemetryPackage{
					Type:  Messages.TelemetryTypeMessagesSent,
					Value: 1,
				}
			}
			if routerMessage.RetrySend < reDeliverCount {
				destinationRoute, ok := router.GetRoute(routerMessage.Route)
				if ok {
					router.deliverMessage(routerMessage, destinationRoute)
					continue
				}
				if router.odometer {
					router.telemetryChannel <- Messages.TelemetryPackage{
						Type:  Messages.TelemetryTypeMessagesResent,
						Value: 1,
					}
				}
				log.Printf("discarting message to route %v due not being registered\n", routerMessage.Route)
			}
			if router.odometer {
				router.telemetryChannel <- Messages.TelemetryPackage{
					Type:  Messages.TelemetryTypeMessagesDiscarded,
					Value: 1,
				}
			}
			log.Println("discarting message due to max retries exceded")
		}
	}
}

// Initializes the router and start the router distribution worker
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
func (router *Router) RegisterRoute(routeKey string, routeMaxSize int) {
	if !router.HasRoute(routeKey) {
		router.routesMutex.Lock()
		route, _ := Route.SetupRoute(router.stopCTX, routeMaxSize)
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

// Returns the router input channel as an pointer
// you should never need this use with caution
// Warning closing this channel will break things without any chance of recover
func (router *Router) GetInputChannel() *chan *Messages.RouterMessage {
	return &router.RouterInput
}

// Creates and returns a new consumer to an route key
// if an route does not exist creates one with DefaultMaxChanSize
// Every consumer is thread safe
// use as many as you need
func (router *Router) GetConsumer(routeKey string) *consumer.Consumer {
	consumer := consumer.Consumer{}
	if !router.HasRoute(routeKey) {
		router.RegisterRoute(routeKey, DefaultMaxRouteSize)
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
func (router *Router) GetPublisher() *SimplePublisher.SimplePublisher {
	publisher := SimplePublisher.SimplePublisher{}
	publisher.StartPublisher(router.GetInputChannel(), router.stopCTX)
	return &publisher
}

// Creates and return a new publisher vinculated to an specific route
// Every message published goes to the router input channel for distribution
// Every dedicated publisher is thread safe
// Use as many as you need
// returns nil if failed to get the route with the routing key
func (router *Router) GetDedicatedPublisher(routeKey string) *DedicatedPublisher.DedicatedPublisher {
	if router.HasRoute(routeKey) {
		publisher := DedicatedPublisher.DedicatedPublisher{}
		route, ok := router.GetRoute(routeKey)
		if !ok {
			return nil
		}
		publisher.StartPublisher(routeKey, route, router.stopCTX)
		return &publisher
	}
	return nil
}

// Checks if the router has an route with that route key
// This locks the Routes map to get the answer
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

func (router *Router) GetTelemetry() Telemetry {
	router.telemetryMutex.Lock()
	telemetryCopy := Telemetry{
		TotalMessages:    router.telemetry.TotalMessages,
		TotalDelivered:   router.telemetry.TotalDelivered,
		TotalLost:        router.telemetry.TotalLost,
		TotalReDelivered: router.telemetry.TotalReDelivered,
	}
	router.telemetryMutex.Unlock()
	return telemetryCopy
}

func (router *Router) telemetryProcessor() {
	for {
		select {
		case <-router.stopCTX.Done():
			return
		case telemetryMessage := <-router.telemetryChannel:
			router.telemetryMutex.Lock()
			router.telemetry.UpdateTelemetry(telemetryMessage)
			router.telemetryMutex.Unlock()
		default:
			time.Sleep(time.Millisecond * 10)
		}

	}
}

func (router *Router) EnableTelemetry() {
	router.odometer = true
	router.telemetry = GetNewTelemetry()
	router.telemetryChannel = make(chan Messages.TelemetryPackage, 1000)
	go router.telemetryProcessor()
}

func (router *Router) StopRoute(routeKey string) bool {
	route, ok := router.GetRoute(routeKey)
	if !ok {
		return false
	}
	route.CloseRoute()
	return true
}

type Telemetry struct {
	// Total messages passed in the input channel
	// fail to delivery counts
	TotalMessages int64
	// Total messages sent to Routes
	TotalDelivered int64
	// Total messages lost
	TotalLost int64
	// Total redelivery attempts
	TotalReDelivered int64
}

func GetNewTelemetry() *Telemetry {
	return &Telemetry{
		TotalMessages:    0,
		TotalDelivered:   0,
		TotalLost:        0,
		TotalReDelivered: 0,
	}
}

func (t *Telemetry) UpdateTelemetry(telemetryData Messages.TelemetryPackage) {
	switch telemetryData.Type {
	case Messages.TelemetryTypeMessagesSent:
		t.TotalMessages += int64(telemetryData.Value)
	case Messages.TelemetryTypeMessagesProcessed:
		t.TotalDelivered += int64(telemetryData.Value)
	case Messages.TelemetryTypeMessagesDiscarded:
		t.TotalLost += int64(telemetryData.Value)
	case Messages.TelemetryTypeMessagesResent:
		t.TotalReDelivered += int64(telemetryData.Value)
	}
}
