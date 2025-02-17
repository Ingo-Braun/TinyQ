package tinyQ

import (

	// "net/http"
	// "os"
	"context"
	"log"
	"sync"
	"time"

	"github.com/Ingo-Braun/TinyQ/consumer"
	"github.com/Ingo-Braun/TinyQ/publishers/simple"
	Messages "github.com/Ingo-Braun/TinyQ/structs/messages"
	Route "github.com/Ingo-Braun/TinyQ/structs/route"
)

const reDeliverCount int = 5
const maxChanSize int = 30

type Router struct {
	Routes        map[string]*Route.Route
	routesMutex   sync.Mutex
	RouterInput   chan *Messages.RouterMessage
	ResendChannel chan *Messages.RouterMessage
	stopCTX       context.Context
	stopCTXCancel context.CancelFunc
}

func (router *Router) deliverMessage(routerMessage *Messages.RouterMessage, destinationRoute *Route.Route) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
writeLoop:
	for {
		select {
		case <-ctx.Done():
			routerMessage.RetrySend++
			router.ResendChannel <- routerMessage
			break writeLoop
		default:

			if len(destinationRoute.Channel) < maxChanSize {
				destinationRoute.Channel <- routerMessage
				break writeLoop
			}
		}
	}
}

func (router *Router) routerInputConsumer(cancelCTX context.Context) {
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

func (router *Router) InitRouter() {
	log.Println("starting router")
	router.RouterInput = make(chan *Messages.RouterMessage)
	router.Routes = make(map[string]*Route.Route)
	router.stopCTX, router.stopCTXCancel = context.WithCancel(context.Background())
	go router.routerInputConsumer(router.stopCTX)
	log.Println("router started")
}

func (router *Router) StopRouter() {
	router.stopCTXCancel()
}

func (router *Router) RegisterRoute(routeKey string) chan *Messages.RouterMessage {
	router.routesMutex.Lock()
	route, outputChannel := Route.SetupRoute()
	router.Routes[routeKey] = route
	router.routesMutex.Unlock()
	return outputChannel
}

func (router *Router) UnregisterRoute(routeKey string) {
	router.routesMutex.Lock()
	delete(router.Routes, routeKey)
	router.routesMutex.Unlock()
}

func (router *Router) GetInputChannel() *chan *Messages.RouterMessage {
	return &router.RouterInput
}

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

func (router *Router) GetRoute(routeKey string) (*Route.Route, bool) {
	router.routesMutex.Lock()
	route, ok := router.Routes[routeKey]
	router.routesMutex.Unlock()
	return route, ok
}

func (router *Router) GetPublisher() *simple.SimplePublisher {
	publisher := simple.SimplePublisher{}
	publisher.StartPublisher(router.GetInputChannel())
	return &publisher
}

func (router *Router) HasRoute(routeKey string) bool {
	_, ok := router.Routes[routeKey]
	return ok
}
