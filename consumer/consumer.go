package consumer

import (
	"context"
	"log"
	"time"

	Messages "github.com/Ingo-Braun/TinyQ/messages"
	Route "github.com/Ingo-Braun/TinyQ/route"
	"github.com/google/uuid"
)

// Milliseconds (ms) to retrive a message or move on
// TODO: allow this value as parameter
const MessageRetrivalTimeout = 200

type Consumer struct {
	// Consumer id used to check responsability on message ack
	id string
	// Route linked to this conssumer
	route *Route.Route
	// Route Closing context if this is done the route has ben closed
	routeCloseCTX context.Context
}

// Attempts to retrive an message from route using MessageRetrivalTimeout const
// Every message retrived starts an delivery timer to Ack
// if the delivery timer expires the message is invalid and becomes avaliable to retrival again
// returns an message and an ok
// if the retrival operation times out returns nil,false
// if the route is closed returns nil,false
func (c *Consumer) GetMessage() (*Messages.RouterMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*MessageRetrivalTimeout)
	defer cancel()
	for {
		select {
		case <-c.routeCloseCTX.Done():
			return nil, false
		case <-ctx.Done():
			log.Println("consumer get message timed out")
			return nil, false
		default:
			routerMessage, ok := c.route.GetMessage(c.GetId())
			return routerMessage, ok
		}
	}

}

// Returns the consumer Unique id (uuid4)
func (c *Consumer) GetId() string {
	return c.id
}

// Initializes the consummer on an route
func (c *Consumer) Setup(route *Route.Route) {
	c.route = route
	c.id = uuid.New().String()
	c.routeCloseCTX = route.CloseCTX
}

// Gets the NOT RELIABLE route size
func (c *Consumer) Size() int {
	return c.route.Size()
}

// Confirm receiving an message
// You can only confirm an message from the same consumer
// This stops the message delivery timer
func (c *Consumer) Ack(message *Messages.RouterMessage) bool {
	return c.route.Ack(c.GetId(), message.GetId())
}
