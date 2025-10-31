package tinyQ

import (
	"context"
	"log"
	"sync"
	"time"

	"errors"

	"github.com/Ingo-Braun/TinyQ/consumer"
	"github.com/Ingo-Braun/TinyQ/hooks"
	Messages "github.com/Ingo-Braun/TinyQ/messages"
	DedicatedPublisher "github.com/Ingo-Braun/TinyQ/publishers/dedicated"
	SimplePublisher "github.com/Ingo-Braun/TinyQ/publishers/simple"
	Route "github.com/Ingo-Braun/TinyQ/route"
	Subscriber "github.com/Ingo-Braun/TinyQ/subscriber"
)

const Version = "v0.10.4-preview"

// N times witch the router will try to deliver
// TODO: allow retry count as an configurable variable
const reDeliverCount int = 5

// default maximum of messages in an Route
const DefaultMaxRouteSize int = 300

// default value for enabling hooks
// this is only used when an defined hooksEnabled exists
const DefaultHooksEnabled bool = false

// Error thrown when attaching hook to an un-registered route
var ErrorRouteToHookNotFound error = errors.New("error finding route to attach hook")

// Error throw when attempting to register a hook when hooks are disabled
var ErrorRouteHooksDisabled error = errors.New("error route hooks is disabled")

// Error throw when attempting to register hooks after router is started
var ErrorRouterStartedHook error = errors.New("error router is started, hooks cannot be enabled while running")

// Main router, responsible to route messages into routes
// call InitRouter to initialize the router
// call StopRouter to stop the routing process WARNING this kills all routes and deletes all messages on the routes
type Router struct {
	// Routing map used to store all known routes [routeKey]*Route
	Routes map[string]*Route.Route
	// Routing map mutex
	routesMutex sync.RWMutex
	// Router input channel - where the messages come
	RouterInput chan *Messages.RouterMessage
	// Router re-send channel not used
	ResendChannel chan *Messages.RouterMessage
	// Router stop context
	stopCTX       context.Context
	stopCTXCancel context.CancelFunc
	// Odometer flag
	odometer bool

	telemetryMutex   sync.Mutex
	telemetry        *Telemetry
	telemetryChannel chan Messages.TelemetryPackage
	// flag if the router is started, used to prevent dangerous operations after the distribution routine is started
	isStarted bool
	// flag if hooks is enabled for this router
	hooksEnabled        bool
	hooksExecutors      map[string]*RouterHookExecutor
	hooksExecutorsMutex sync.Mutex
}

// Ad-hoc message deliver delivery`s a message without the need to use an publisher
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
				if router.hooksEnabled {
					router.hooksExecutorsMutex.Lock()
					routeHookExecutor, ok := router.hooksExecutors[routerMessage.Route]
					if ok {
						messageCopy := *routerMessage
						routeHookExecutor.HookInputChannel <- &messageCopy
					}
					router.hooksExecutorsMutex.Unlock()
				}
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
// messages with an invalid destination are discarded
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
				router.routesMutex.RLock()
				destinationRoute, ok := router.getRoute(routerMessage.Route)
				if ok {
					router.deliverMessage(routerMessage, destinationRoute)
					router.routesMutex.RUnlock()
					continue
				}
				router.routesMutex.RUnlock()
				if router.odometer {
					router.telemetryChannel <- Messages.TelemetryPackage{
						Type:  Messages.TelemetryTypeMessagesResent,
						Value: 1,
					}
				}
				log.Printf("discarding message to route %v due not being registered\n", routerMessage.Route)
			}
			if router.odometer {
				router.telemetryChannel <- Messages.TelemetryPackage{
					Type:  Messages.TelemetryTypeMessagesDiscarded,
					Value: 1,
				}
			}
			log.Println("discarding message due to max retries exceeded")
		}
	}
}

// Initializes the router and start the router distribution worker
// Call this BEFORE using anything from the router
// NIL pointer exceptions will be raised if not called before use
func (router *Router) InitRouter() {
	log.Println("starting router")
	router.RouterInput = make(chan *Messages.RouterMessage)
	router.Routes = make(map[string]*Route.Route)
	router.stopCTX, router.stopCTXCancel = context.WithCancel(context.Background())
	go router.routerDistributionWorker(router.stopCTX)
	router.isStarted = true
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

	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	router.registerRoute(routeKey, routeMaxSize)
}

// performs the register route unsafe
// DO NOT CALL this if routesMutex lock is not acquired
func (router *Router) registerRoute(routeKey string, routeMaxSize int) {
	if !router.hasRoute(routeKey) {
		route, _ := Route.SetupRoute(router.stopCTX, routeMaxSize)
		router.Routes[routeKey] = route
	}
}

// Removes an route
// This will stop all consumers connected to this route and deletes all messages
// Calling twice on same route key is fine
func (router *Router) UnregisterRoute(routeKey string) bool {
	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	route, ok := router.Routes[routeKey]
	if ok {
		route.CloseRoute()
		if router.hooksEnabled {
			router.hooksExecutorsMutex.Lock()
			defer router.hooksExecutorsMutex.Unlock()
			executor, ok := router.hooksExecutors[routeKey]
			if ok {
				executor.HookExecutor.Stop()
			}
			delete(router.hooksExecutors, routeKey)
		}
		delete(router.Routes, routeKey)
		return true
	}
	return false
}

// Returns the router input channel as an pointer
// you should never need this use with caution
// Warning closing this channel will break things without any chance of recover
func (router *Router) GetInputChannel() *chan *Messages.RouterMessage {
	return &router.RouterInput
}

// Creates and returns a new consumer to an route key
// if an route does not exist creates one with DefaultMaxChanSize and hooks DISABLED
// Every consumer is thread safe
// use as many as you need
func (router *Router) GetConsumer(routeKey string) *consumer.Consumer {
	consumer := consumer.Consumer{}
	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	if !router.hasRoute(routeKey) {
		router.registerRoute(routeKey, DefaultMaxRouteSize)
	}
	route, ok := router.getRoute(routeKey)
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
	defer router.routesMutex.Unlock()
	route, ok := router.getRoute(routeKey)
	return route, ok
}

// performs the get route unsafe
// DO NOT CALL this if routesMutex lock is not acquired
func (router *Router) getRoute(routeKey string) (*Route.Route, bool) {
	route, ok := router.Routes[routeKey]
	return route, ok
}

// Creates and return a new publisher linked to this router
// Every message published goes to the router input channel for distribution
// Every publisher is thread safe
// Use as many as you need
func (router *Router) GetPublisher() *SimplePublisher.SimplePublisher {
	publisher := SimplePublisher.SimplePublisher{}
	publisher.StartPublisher(router.GetInputChannel(), router.stopCTX)
	return &publisher
}

// Creates and return a new publisher linked to an specific route
// Every message published goes to the router input channel for distribution
// Every dedicated publisher is thread safe
// Use as many as you need
// returns nil if failed to get the route with the routing key
func (router *Router) GetDedicatedPublisher(routeKey string) *DedicatedPublisher.DedicatedPublisher {
	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	if router.hasRoute(routeKey) {
		publisher := DedicatedPublisher.DedicatedPublisher{}
		route, ok := router.getRoute(routeKey)
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
	router.routesMutex.RLock()
	ok := router.hasRoute(routeKey)
	router.routesMutex.RUnlock()
	return ok
}

// performs the has route check unsafe
// DO NOT CALL this if routesMutex lock is not acquired
func (router *Router) hasRoute(routeKey string) bool {

	_, ok := router.Routes[routeKey]
	return ok
}

// Checks if the router is running
func (router *Router) IsRunning() bool {
	return !errors.Is(router.stopCTX.Err(), context.Canceled)
}

func (router *Router) GetSubscriber(routeKey string, callBack Subscriber.CallBack) (*Subscriber.Subscriber, bool) {
	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	if router.hasRoute(routeKey) {
		subscriber := Subscriber.Subscriber{}
		route, ok := router.getRoute(routeKey)
		if !ok {
			return nil, false
		}
		subscriber.Setup(route, callBack)
		return &subscriber, true
	}
	return nil, false
}

// return an Telemetry object with the data collected from the router
// Warning this function data is an snapshot of data
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

// telemetry routine to receive telemetry information from the distribution routine
func (router *Router) telemetryProcessor() {
	for {
		select {
		case <-router.stopCTX.Done():
			return
		case telemetryMessage := <-router.telemetryChannel:
			router.telemetryMutex.Lock()
			router.telemetry.UpdateTelemetry(telemetryMessage)
			router.telemetryMutex.Unlock()

		}

	}
}

// enables telemetry data collection
// for more information on what values are generated see Telemetry struct
func (router *Router) EnableTelemetry() {
	router.odometer = true
	router.telemetry = GetNewTelemetry()
	router.telemetryChannel = make(chan Messages.TelemetryPackage, 1000000)
	go router.telemetryProcessor()
}

// stops an specific route
// WARNING this will cascade to all consumers and subscribers linked to this route
func (router *Router) StopRoute(routeKey string) bool {
	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	route, ok := router.getRoute(routeKey)
	if !ok {
		return false
	}
	route.CloseRoute()
	if router.hooksEnabled {
		router.hooksExecutorsMutex.Lock()
		defer router.hooksExecutorsMutex.Unlock()
		executor, ok := router.hooksExecutors[routeKey]
		if ok {
			executor.HookExecutor.Stop()
		}
		delete(router.hooksExecutors, routeKey)
	}
	return true
}

// enables hooks
// can only be called when router.InitRouter has been not called yet
// returns ErrorRouterStartedHook when called after router start
func (router *Router) EnableHooks() error {
	if router.isStarted {
		return ErrorRouterStartedHook
	}
	router.hooksEnabled = true
	router.hooksExecutors = make(map[string]*RouterHookExecutor)
	return nil
}

// returns if hooks are enabled
func (router *Router) IsHooksEnabled() bool {
	return router.hooksEnabled
}

// add hook to run after an message is acknowledged
// enables hooks in route if hooks are enabled on the router
// returns ErrorRouteToHookNotFound if an route is not found with the specified route key
func (router *Router) AddPostAckHook(routeKey string, hook hooks.Hook) error {
	if !router.hooksEnabled {
		return ErrorRouteHooksDisabled
	}
	router.routesMutex.Lock()
	defer router.routesMutex.Unlock()
	route, ok := router.getRoute(routeKey)
	if ok {
		err := route.AddHook(hook)
		return err

	} else {
		return ErrorRouteToHookNotFound
	}

}

func (router *Router) AddMessagePostInHook(routeKey string, hook hooks.Hook) error {
	router.routesMutex.Lock()
	if !router.hasRoute(routeKey) {
		router.routesMutex.Unlock()
		return ErrorRouteToHookNotFound
	}
	router.routesMutex.Unlock()
	router.hooksExecutorsMutex.Lock()
	defer router.hooksExecutorsMutex.Unlock()
	routerExecutor, ok := router.hooksExecutors[routeKey]
	if !ok {
		routerExecutor = &RouterHookExecutor{}
		routerExecutor.HookExecutor = hooks.CreateHookExecutor(true)
		routerExecutor.HookInputChannel = routerExecutor.HookExecutor.GetInputChannel()
		routerExecutor.HookExecutor.StartExecutor()
		router.hooksExecutors[routeKey] = routerExecutor
	}
	routerExecutor.HookExecutor.AddHook(hook)
	return nil
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

// Telemetry generates and return an empty Telemetry object
// Use router.GetTelemetry to get accurate telemetry data
func GetNewTelemetry() *Telemetry {
	return &Telemetry{
		TotalMessages:    0,
		TotalDelivered:   0,
		TotalLost:        0,
		TotalReDelivered: 0,
	}
}

// function to update data on telemetry object
// Use router.GetTelemetry to get accurate telemetry data
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

type RouterHookExecutor struct {
	HookExecutor     *hooks.HookExecutor
	HookInputChannel hooks.HookChannel
}
