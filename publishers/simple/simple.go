package simple

import (
	"context"

	Messages "github.com/Ingo-Braun/TinyQ/messages"
)

type SimplePublisher struct {
	// Router messages input channel
	outputChan chan *Messages.RouterMessage
	// Router stop context
	routerStopCTX context.Context
}

// Creates and send a new Message publishes and return its id and ok (bool)
// Returns empty string and false if the router has ben stopped "calling router.StopRouter()"
func (pub *SimplePublisher) Publish(content []byte, RouteKey string) (string, bool) {
	select {
	case <-pub.routerStopCTX.Done():
		return "", false
	default:
		routerMessage := Messages.CreateMessage(content, RouteKey)
		messageId := routerMessage.GetId()
		pub.outputChan <- routerMessage
		return messageId, true
	}
}

// Starts an publisher receiving the router input channel and the router stop context
func (pub *SimplePublisher) StartPublisher(output *chan *Messages.RouterMessage, routerStopCTX context.Context) {
	pub.routerStopCTX = routerStopCTX
	pub.outputChan = *output
}
