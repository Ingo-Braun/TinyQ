package dedicated

import (
	"context"

	Messages "github.com/Ingo-Braun/TinyQ/messages"
	Route "github.com/Ingo-Braun/TinyQ/route"
)

type DedicatedPublisher struct {
	// Router stop context
	routerStopCTX context.Context
	//
	route    *Route.Route
	routeKey string
}

// Creates and send a new Message publishes and return its id and ok (bool)
// Returns empty string and false if the router has ben stopped "calling router.StopRouter()"
func (pub *DedicatedPublisher) Publish(content []byte) (string, bool) {
	select {
	case <-pub.routerStopCTX.Done():
		return "", false
	default:
		if pub.route.IsActive() {
			routerMessage := Messages.CreateMessage(content, pub.routeKey)
			messageId := routerMessage.GetId()
			pub.route.Channel <- routerMessage
			return messageId, true
		}
		return "", false
	}
}

// Starts an publisher receiving the router input channel and the router stop context
func (pub *DedicatedPublisher) StartPublisher(routeKey string, route *Route.Route, routerStopCTX context.Context) {
	pub.routerStopCTX = routerStopCTX
	pub.routeKey = routeKey
	pub.route = route
}

// check if the route is active
// if the route is not active than there is no reason this should be active
func (pub *DedicatedPublisher) IsActive() bool {
	return pub.route.IsActive()
}
