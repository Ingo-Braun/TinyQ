package consumer

import (
	"context"
	"log"
	"time"

	Messages "github.com/Ingo-Braun/TinyQ/structs/messages"
	Route "github.com/Ingo-Braun/TinyQ/structs/route"
)

type Consumer struct {
	route *Route.Route
	// inputChannel chan Messages.RouterMessage
	// ctx          context.Context
	// Cancel       context.CancelFunc
}

func (c *Consumer) GetMessage() (*Messages.RouterMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Println("consumer get message timed out")
			return nil, false
		default:
			routerMessage, ok := c.route.GetMessage()
			return routerMessage, ok
		}
	}

}

func (c *Consumer) Setup(route *Route.Route) {
	c.route = route
}

func (c *Consumer) Size() int {
	return c.route.Size()
}
