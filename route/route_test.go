package route_test

import (
	"context"
	"testing"
	"time"

	"github.com/Ingo-Braun/TinyQ/messages"
	route "github.com/Ingo-Braun/TinyQ/route"
)

// creates a new Route using context
// used as shortcut for tests
func createTestRoute(ctx context.Context) (*route.Route, chan *messages.RouterMessage) {

	newRoute, routeChannel := route.SetupRoute(ctx, 10)
	return newRoute, routeChannel
}

// test creating new route
// fails if created route is closed
// fails if route input channel is nil
func TestCreateRoute(t *testing.T) {
	t.Parallel()
	t.Log("creating route")
	context, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := route.SetupRoute(context, 10)
	if newRoute.IsClosed() {
		t.Error("test failed new route is closed, expected open")
		t.FailNow()
	}
	if routeChannel == nil {
		t.Error("test failed router channel is nil")
		t.FailNow()
	}
}

// test add message to route and get route message queue size
// fails if route size is zero
func TestAddMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	defer newRoute.CloseRoute()
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	if newRoute.Size() == 0 {
		t.Error("test failed route size is zero")
		t.FailNow()
	}

}

// test getting published message
// fails if route size is zero
// fails if route does not return an message
// fails if retrieved message id does not match original message id
func TestGetMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	if newRoute.Size() == 0 {
		t.Error("test failed route size is zero")
		t.FailNow()
	}
	retrievedMessage, ok := newRoute.GetMessage("consumer1")
	if !ok {
		t.Error("test failed route did not return an message")
		t.FailNow()
	}
	if message.GetId() != retrievedMessage.GetId() {
		t.Errorf("test failed retrieved message id mismatch want %v got %v \n", message.GetId(), retrievedMessage.GetId())
		t.FailNow()
	}

}

// test acknowledge message
// fails if fails to retrieve message
// fails if retrieved message id does not match original message id
// fails if acknowledge message returns false
func TestAck(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	consumerId := "test"
	retrievedMessage, ok := newRoute.GetMessage(consumerId)
	if !ok {
		t.Error("test failed retrieving message")
		t.FailNow()
	}
	if retrievedMessage.GetId() != message.GetId() {
		t.Errorf("test failed retrieved message id mismatch wanted %v got %v\n", message.GetId(), retrievedMessage.GetId())
		t.FailNow()
	}
	ok = newRoute.Ack(consumerId, retrievedMessage.GetId())
	if !ok {
		t.Error("test failed ACK")
		t.FailNow()
	}
}

// test if retrieved message is tied to the same consumer
// fails if can not retrieve message
// fails if retrieved message id does not match original message id
// fails if can not get consumer id tied to message
// fails if consumer id does not match consumer id tied to retrieved message
func TestConsumerLock(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	consumerId := "test"
	retrievedMessage, ok := newRoute.GetMessage(consumerId)
	if !ok {
		t.Error("test failed retrieving message")
		t.FailNow()
	}
	if retrievedMessage.GetId() != message.GetId() {
		t.Errorf("test failed retrieved message id mismatch wanted %v got %v\n", message.GetId(), retrievedMessage.GetId())
		t.FailNow()
	}
	var messageConsumerId string
	messageConsumerId, ok = newRoute.GetConsumerId(retrievedMessage)
	if !ok {
		t.Error("failed getting consumer ID from message")
		t.FailNow()
	}
	if messageConsumerId != consumerId {
		t.Errorf("test failed consumer id mismatch wanted %v got %v \n", consumerId, messageConsumerId)
		t.FailNow()
	}
}

// test if an message is in acknowledge wait list
// fails if can not retrieve message
// fails if retrieved message id does not match original message id
// fails if retrieved message is not in acknowledge wait list
func TestMessageInAckWaitList(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	consumerId := "test"
	retrievedMessage, ok := newRoute.GetMessage(consumerId)
	if !ok {
		t.Error("test failed retrieving message")
		t.FailNow()
	}
	if retrievedMessage.GetId() != message.GetId() {
		t.Errorf("test failed retrieved message id mismatch wanted %v got %v\n", message.GetId(), retrievedMessage.GetId())
		t.FailNow()
	}
	ok = newRoute.IsWaitingAck(retrievedMessage.GetId())
	if !ok {
		t.Error("test failed retrieved message is not in ack wait list")
		t.Fail()
	}
}

// test if route closes if router context is closed
// fails if route is not closed after context close
func TestCloseRouteByRouterContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	cancel()
	time.Sleep(time.Millisecond * 100)
	if !newRoute.IsClosed() {
		t.Error("test failed route is running after router context close")
		t.FailNow()
	}

}

// test message re-delivery
// fails if can not retrieve message
// fails if retrieved message is in acknowledge wait list after expire
// fails if retrieved message after expire has different message if from original message
func TestReDelivery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	retrievedMessage, ok := newRoute.GetMessage("test")
	if !ok {
		t.Error("test failed in retrieve message")
		t.FailNow()
	}
	time.Sleep(messages.DeliveryTimeout + (time.Millisecond * 200))
	isMessageAwaitingAck := newRoute.IsWaitingAck(retrievedMessage.GetId())
	if isMessageAwaitingAck {
		t.Error("test failed message is still awaiting ack")
		t.FailNow()
	}
	reRetrievedMessage, ok := newRoute.GetMessage("test")
	if !ok {
		t.Error("test failed in retrieve message")
		t.FailNow()
	}
	if message.GetId() != reRetrievedMessage.GetId() {
		t.Errorf("test failed message id mismatch wanted %v got %v \n", message.GetId(), reRetrievedMessage.GetId())
		t.FailNow()
	}
}

// test ack with other consumer id
// fails if can not retrieve message
// fails if route returns true on ack with other consumer id
func TestAckMessageFromOtherConsumer(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeChannel := createTestRoute(ctx)
	message := messages.CreateMessage([]byte("test"), "testRoute")
	routeChannel <- message
	consumerId1 := "consumer1"
	consumerId2 := "consumer2"
	retrievedMessage, ok := newRoute.GetMessage(consumerId1)
	if !ok {
		t.Error("test failed in retrieve message")
		t.FailNow()
	}
	ok = newRoute.Ack(consumerId2, retrievedMessage.GetId())
	if ok {
		t.Error("test failed route allowed other consumer to ack message")
		t.FailNow()
	}
}

// test if hooks are enabled
// fails if hooks are enabled by default
func TestHooksEnabled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	ok := newRoute.IsHooksEnabled()
	if ok {
		t.Error("test failed hooks should be disabled by default")
		t.FailNow()
	}
}

// test enabling hooks
// fails if hooks are disabled after enabling
func TestEnablingHooks(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	newRoute.EnableHooks()
	ok := newRoute.IsHooksEnabled()
	if !ok {
		t.Error("test failed hooks are not enabled")
		t.FailNow()
	}
}

// test add hooks
// fails if adding hook returns error
func TestAddHook(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	hook1 := func(message *messages.RouterMessage) error {
		return nil
	}
	err := newRoute.AddHook(hook1)
	if err != nil {
		t.Errorf("test failed adding hook returned error %v \n", err)
		t.FailNow()
	}
}

// test hooks count
// fails if adding hook returns error
// fails if hooks count is not 1 after adding 1 hook
func TestHookCount(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	hook1 := func(message *messages.RouterMessage) error {
		return nil
	}
	err := newRoute.AddHook(hook1)
	if err != nil {
		t.Errorf("test failed adding hook returned error %v \n", err)
		t.FailNow()
	}
	hookCount := newRoute.HooksCount()
	if hookCount != 1 {
		t.Errorf("test failed hook count mismatch wanted %v got %v \n", 1, hookCount)
	}
}

// test if added hooks are being executed
// fails if adding hook returns error
// fails if hook count is not 1 after adding hook
// fails if can not retrieve message from route
// fails if can not acknowledge retrieved message from route
// fails if hook does not execute in time
// fails if message id retrieved from hook is not the same as original
func TestHookExecution(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeInputChannel := createTestRoute(ctx)
	hookReturnChannel := make(chan string, 1)
	hook1 := func(message *messages.RouterMessage) error {
		hookReturnChannel <- message.GetId()
		return nil
	}
	err := newRoute.AddHook(hook1)
	if err != nil {
		t.Errorf("test failed adding hook returned error %v \n", err)
		t.FailNow()
	}
	hookCount := newRoute.HooksCount()
	if hookCount != 1 {
		t.Errorf("test failed hook count mismatch wanted %v got %v \n", 1, hookCount)
	}
	message := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- message
	time.Sleep(time.Millisecond * 100)
	retrievedMessage, ok := newRoute.GetMessage("test")
	if !ok {
		t.Error("test failed in retrieve message")
		t.FailNow()
	}
	ok = newRoute.Ack("test", retrievedMessage.GetId())
	if !ok {
		t.Error("test failed in ack retrieved message")
		t.FailNow()
	}
	hookCtx, hookCancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer hookCancel()
	var hookMessageId string
hookLoop:
	for {
		select {
		case <-hookCtx.Done():
			t.Error("test failed hook execution timeout")
			t.FailNow()
		case hookMessageId = <-hookReturnChannel:
			break hookLoop
		}
	}
	if message.GetId() != hookMessageId {
		t.Errorf("test failed hook message id mismatch wanted %v got %v \n", message.GetId(), hookMessageId)
		t.FailNow()
	}

}

// test getting consumer id from an unsent message
// fails if return true on getting consumer id from an unsent message
func TestGetConsumerFromUnsentMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, routeInputChannel := createTestRoute(ctx)
	msg := messages.CreateMessage([]byte("test"), "test")
	routeInputChannel <- msg
	_, ok := newRoute.GetConsumerId(msg)
	if ok {
		t.Error("test failed get consumer from an message not consumed should return false")
		t.FailNow()
	}

}

// test calling GetMessage while empty
// fails if GetMessage returns success on empty route
func TestGetMessageWileEmpty(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	_, ok := newRoute.GetMessage("test")
	if ok {
		t.Error("test failed getting message while empty should return false")
		t.FailNow()
	}

}

// test getting hooks count while hooks being disabled
// fails if returned hooks count is not -1
func TestGetHooksCountWithHooksDisabled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newRoute, _ := createTestRoute(ctx)
	hookCount := newRoute.HooksCount()
	if hookCount != -1 {
		t.Error("test failed hooks count if hooks are disabled should return -1")
		t.FailNow()
	}
}
