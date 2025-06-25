package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/Ingo-Braun/TinyQ/consumer"
	"github.com/Ingo-Braun/TinyQ/messages"
	"github.com/Ingo-Braun/TinyQ/route"
)

func createConsumer(ctx context.Context) (*consumer.Consumer, *route.Route, chan *messages.RouterMessage) {
	newRoute, routeChannel := route.SetupRoute(ctx, 10)
	newConsumer := consumer.Consumer{}
	newConsumer.Setup(newRoute)
	return &newConsumer, newRoute, routeChannel
}

func TestCreateConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := route.SetupRoute(ctx, 10)
	newConsumer := consumer.Consumer{}
	newConsumer.Setup(newRoute)
	if newConsumer.IsClosed() {
		t.Error("test failed new Consumer is closed")
		t.FailNow()
	}
}

func TestGetMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer, _, routeInputChannel := createConsumer(ctx)
	if newConsumer.IsClosed() {
		t.Error("test failed new Consumer is closed")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	retrievedMessage, ok := newConsumer.GetMessage()
	if !ok {
		t.Error("test failed retrieve message ")
		t.FailNow()
	}
	if retrievedMessage.GetId() != msg.GetId() {
		t.Errorf("test failed retrieved message id mismatch wanted %v got %v \n", msg.GetId(), retrievedMessage.GetId())
		t.FailNow()
	}
}

func TestRouteTiedSize(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer, newRoute, routeInputChannel := createConsumer(ctx)
	if newConsumer.IsClosed() {
		t.Error("test failed new Consumer is closed")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	if newRoute.Size() != newConsumer.Size() {
		t.Errorf("test failed consumer size is different from route size, expected %v got %v \n", newConsumer.Size(), newRoute.Size())
		t.FailNow()
	}
}

func TestConsumerTiedMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer, newRoute, routeInputChannel := createConsumer(ctx)
	if newConsumer.IsClosed() {
		t.Error("test failed new Consumer is closed")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	retrievedMessage, ok := newConsumer.GetMessage()
	if !ok {
		t.Error("test failed GetMessage returned false")
		t.FailNow()
	}
	consumerId := newConsumer.GetId()
	retrievedMessageConsumerId, ok := newRoute.GetConsumerId(retrievedMessage)
	if !ok {
		t.Error("test failed , getting consumer id from route returned false")
		t.FailNow()
	}
	if consumerId != retrievedMessageConsumerId {
		t.Errorf("test failed retrieved message consumer id mismatch wanted %v got %v \n", consumerId, retrievedMessageConsumerId)
		t.FailNow()
	}
}

func TestAckByMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer, newRoute, routeInputChannel := createConsumer(ctx)
	if newConsumer.IsClosed() {
		t.Error("test failed new Consumer is closed")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	retrievedMessage, ok := newConsumer.GetMessage()
	if !ok {
		t.Error("test failed GetMessage returned false")
		t.FailNow()
	}
	if !newRoute.IsWaitingAck(retrievedMessage.GetId()) {
		t.Error("test failed retrieved message is not in route wait ack list")
		t.FailNow()
	}
	if !newConsumer.Ack(retrievedMessage) {
		t.Error("test failed ack by message returned false")
		t.FailNow()
	}
}

func TestAckById(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer, newRoute, routeInputChannel := createConsumer(ctx)
	if newConsumer.IsClosed() {
		t.Error("test failed new Consumer is closed")
		t.FailNow()
	}
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	retrievedMessage, ok := newConsumer.GetMessage()
	if !ok {
		t.Error("test failed GetMessage returned false")
		t.FailNow()
	}
	if !newRoute.IsWaitingAck(retrievedMessage.GetId()) {
		t.Error("test failed retrieved message is not in route wait ack list")
		t.FailNow()
	}
	if !newConsumer.AckByID(retrievedMessage.GetId()) {
		t.Error("test failed ack by Id returned false")
		t.FailNow()
	}
}

func TestGettingMessageWithEmptyRoute(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer, _, _ := createConsumer(ctx)
	retrievedMessage, ok := newConsumer.GetMessage()
	if ok {
		t.Error("test failed GetMessage with empty route should return false")
		t.FailNow()
	}
	if retrievedMessage != nil {
		t.Error("test failed GetMessage returned a non nil message with empty route")
		t.FailNow()
	}
}

func TestGettingMessageAfterRouteClose(t *testing.T) {
	t.Parallel()
	ctx, cancelRoute := context.WithCancel(context.Background())
	newConsumer, newRoute, _ := createConsumer(ctx)
	cancelRoute()
	time.Sleep(time.Millisecond * 100)
	if !newRoute.IsClosed() {
		t.Error("test failed route is not closed after calling close context")
		t.FailNow()
	}
	if !newConsumer.IsClosed() {
		t.Error("test failed consumer is not closed after route close")
		t.FailNow()
	}
	retrievedMessage, ok := newConsumer.GetMessage()
	if ok {
		t.Error("test failed GetMessage with closed route should return false")
		t.FailNow()
	}
	if retrievedMessage != nil {
		t.Error("test failed GetMessage returned a non nil message with closed route")
		t.FailNow()
	}
}
