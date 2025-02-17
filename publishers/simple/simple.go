package simple

import (
	Messages "github.com/Ingo-Braun/TinyQ/structs/messages"
)

type SimplePublisher struct {
	outputChan chan *Messages.RouterMessage
}

func (pub *SimplePublisher) Publish(content []byte, RouteKey string) {
	// routerMessage := Messages.RouterMessage{
	// 	Route:     RouteKey,
	// 	Content:   content,
	// 	RetrySend: 0,
	// }
	routerMessage := Messages.CreateMessage(content, RouteKey)
	pub.outputChan <- routerMessage
}

func (pub *SimplePublisher) StartPublisher(output *chan *Messages.RouterMessage) {
	pub.outputChan = *output
}
