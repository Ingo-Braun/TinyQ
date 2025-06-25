package subscriber_test

import (
	"context"
	"testing"
	"time"

	"github.com/Ingo-Braun/TinyQ/messages"
	"github.com/Ingo-Braun/TinyQ/route"
	"github.com/Ingo-Braun/TinyQ/subscriber"
)

func TestCreateSubscriber(t *testing.T) {
	t.Log("creating subscriber")
	routeCtx, routeCancel := context.WithCancel(context.Background())
	defer routeCancel()
	newRoute, _ := route.SetupRoute(routeCtx, 10)
	returnChannel := make(chan string, 5)
	callback := func(message *messages.RouterMessage, ack context.CancelFunc) {
		returnChannel <- message.GetId()
		ack()
	}
	newSubscriber := subscriber.Subscriber{}
	newSubscriber.Setup(newRoute, callback)
	if !newSubscriber.IsRunning() {
		t.Error("test failed subscriber is not running")
		t.FailNow()
	}

}

func TestReceiveMessage(t *testing.T) {
	t.Parallel()
	t.Log("creating subscriber")
	routeCtx, routeCancel := context.WithCancel(context.Background())
	defer routeCancel()
	newRoute, routeInputChannel := route.SetupRoute(routeCtx, 10)
	returnChannel := make(chan string, 5)
	callback := func(message *messages.RouterMessage, ack context.CancelFunc) {
		returnChannel <- message.GetId()
		ack()
	}
	newSubscriber := subscriber.Subscriber{}
	newSubscriber.Setup(newRoute, callback)
	if newSubscriber.IsClosed() {
		t.Error("test failed subscriber is not running")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	time.Sleep(time.Millisecond * 100)
	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer receiveCancel()
	var messageId string
receiveLoop:
	for {
		select {
		case <-receiveCtx.Done():
			t.Error("test failed receive time out")
			t.Fail()
		case messageId = <-returnChannel:
			receiveCancel()
			break receiveLoop
		}
	}
	if messageId != msg.GetId() {
		t.Errorf("test failed message id mismatch wanted %v got %v \n", msg.GetId(), messageId)
		t.FailNow()
	}

}

func TestFailToAck(t *testing.T) {
	t.Parallel()
	t.Log("creating subscriber")
	routeCtx, routeCancel := context.WithCancel(context.Background())
	defer routeCancel()
	newRoute, routeInputChannel := route.SetupRoute(routeCtx, 10)
	returnChannel := make(chan string, 5)
	callback := func(message *messages.RouterMessage, ack context.CancelFunc) {
		t.Log("hook executed awaiting for message to expire")
		time.Sleep(messages.DeliveryTimeout + 1)
		t.Logf("message is expired %v \n", message.IsExpired())
		t.Logf("message is valid %v \n", message.IsValid())
		ack()
		returnChannel <- message.GetId()
	}
	newSubscriber := subscriber.Subscriber{}
	newSubscriber.Setup(newRoute, callback)
	if newSubscriber.IsClosed() {
		t.Error("test failed subscriber is not running")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	t.Log("message published")
	routeInputChannel <- msg
	time.Sleep(time.Millisecond * 100)
	receiveCtx, receiveCancel := context.WithTimeout(context.Background(), messages.DeliveryTimeout+2)
	defer receiveCancel()
	var messageId string
receiveLoop:
	for {
		select {
		case <-receiveCtx.Done():
			t.Error("test failed receive time out")
			t.Fail()
			break receiveLoop
		case messageId = <-returnChannel:
			receiveCancel()
			break receiveLoop
		}
	}
	time.Sleep(time.Millisecond * 100)
	subscriberId, ok := newRoute.GetConsumerId(msg)
	if ok {
		t.Log(subscriberId)
		t.Error("test failed subscriber is holding past expire")
		t.FailNow()
	}
	if newRoute.IsWaitingAck(messageId) {
		t.Error("test failed message should be expired")
		t.FailNow()
	}
}
