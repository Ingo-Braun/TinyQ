package consumer

import (
	"context"
	"log"
	"time"

	Messages "github.com/Ingo-Braun/TinyQ/structs/messages"
	Route "github.com/Ingo-Braun/TinyQ/structs/route"
	"github.com/google/uuid"
)

type Consumer struct {
	route *Route.Route
	id    string
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
			routerMessage, ok := c.route.GetMessage(c.GetId())
			return routerMessage, ok
		}
	}

}

func (c *Consumer) GetId() string {
	return c.id
}

func (c *Consumer) Setup(route *Route.Route) {
	c.route = route
	c.id = uuid.New().String()
}

func (c *Consumer) Size() int {
	return c.route.Size()
}

func (c *Consumer) Ack(message *Messages.RouterMessage) bool {
	return c.route.Ack(c.GetId(), message.GetId())
}
