package tinyQ_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"testing"
	"time"

	tinyQ "github.com/Ingo-Braun/TinyQ"
	"github.com/Ingo-Braun/TinyQ/consumer"
	"github.com/Ingo-Braun/TinyQ/hooks"
	"github.com/Ingo-Braun/TinyQ/messages"
)

// starts a new router
func startRouter() *tinyQ.Router {
	router := tinyQ.Router{}
	router.InitRouter()
	return &router
}

// helper function to get messages from an consumer
// consumer should timeout before
func receiveMessage(consumer *consumer.Consumer) (*messages.RouterMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2000)
	for {
		select {
		case <-ctx.Done():
			cancel()
			return nil, false
		default:
			message, ok := consumer.GetMessage()
			if ok {
				cancel()
				return message, ok
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// message struct to use as content to messages tests
// json serializable
type Msg struct {
	WorkerId  int `json:"worker_id"`
	MessageId int `json:"message_id"`
}

// Tests creating a new router
// Fails if the router is not running
func TestCreateRouter(t *testing.T) {
	t.Log("starting router")
	router := tinyQ.Router{}
	router.InitRouter()
	if router.IsRunning() == false {
		t.Error("error router is not running")
		t.FailNow()
	}
	t.Log("stopping router")
	router.StopRouter()
}

// Tests creating a new Route
// Fails if the router does not have the created route
func TestCreateRoute(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	t.Log("creating route test")
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)
	if !router.HasRoute("test") {
		t.Error("failed to create route")
	}
}

// Tests creating an publisher for an route
// fails if the publisher is nil
func TestCreatePublisher(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)
	publisher := router.GetPublisher()
	if publisher == nil {
		t.Error("failed to create Publisher")
		t.FailNow()
	}
}

// Tests creating an consumer
// fails if the consumer is nil
func TestCreateBasicConsumer(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)
	consumer := router.GetConsumer("test")
	if consumer == nil {
		t.Error("failed to create Consumer")
		t.FailNow()
	}
}

// Tests if creating an Consumer to an not registered route automatically creates the route
// fails if the consumer is nil
// fails if the router does not create the route automatically
func TestCreateBasicConsumerNotRegisteredRoute(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	consumer := router.GetConsumer("test")
	if consumer == nil {
		t.Error("failed to create Consumer")
		t.FailNow()
	}
	if router.HasRoute("test") {
		t.Log("router created test route automatically")
	}
}

// Test sending an message
// fails if there is an failure in delivering the message
// fails if the route size is 0
// the reading of the Route size is reliable due to not having an consumer attached to it
func TestSend(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// get the Route struct
	route, ok := router.GetRoute("test")

	if ok {
		// delay to wait for the Router delivery worker
		// to pass the message from the input channel to the destination Route
		time.Sleep(time.Millisecond * 100)

		if route.Size() == 0 {
			t.Logf("route did not receive message queue size %v", route.Size())
			t.FailNow()
		}
	}
	t.Logf("route received message!, queue size %v", route.Size())
}

// Tests sending an message and receiving using an consumer
// fails if there is an failure in delivering the message
// fails if there is an failure in retrieving the message
// fails if the message content is different from the original
// fails if the message id is different from the id returned on publish
func TestSendAndReceive(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new Consumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	messageId, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	// get the Route struct
	route, _ := router.GetRoute("test")

	// logs the queue size
	// this reading should be reliable since no message has been consumed from the Route
	t.Logf("queue size %v", route.Size())

	// retrieves the message
	message, ok := receiveMessage(consumer)
	if !ok {
		t.Error("failed retrieving the message")
		t.FailNow()
	}
	if string(message.Content) != messageText {
		t.Errorf("failed receiving message sent %v received %v", messageText, string(message.Content))
		t.FailNow()
	}
	if message.GetId() != messageId {
		t.Errorf("message id mismatch expected %v got %v", messageId, message.GetId())
		t.FailNow()
	}
}

// Tests message Ack
// fails if there is an failure in delivering the message
// fails if there is an failure in retrieving the message
// fails if Ack returns not nil
func TestAck(t *testing.T) {
	t.Parallel()
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new Consumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	message, ok := receiveMessage(consumer)
	if !ok {
		t.Error("failed retrieving the message")
		t.FailNow()
	}
	if ok && string(message.Content) == messageText {
		t.Log("testing ack")
		t.Logf("is message expired %v", message.IsExpired())
		t.Logf("is message valid %v", message.IsValid())
		if message.IsExpired() == false && message.IsValid() {
			err := consumer.Ack(message)
			if !err {
				t.FailNow()
			}
			route, _ := router.GetRoute("test")
			if route.IsWaitingAck(message.GetId()) {
				t.Error("test failed message is in awaiting ack")
				t.FailNow()
			}
		}
	} else if ok && string(message.Content) != messageText {
		t.Errorf("failed receiving message sent %v received %v", messageText, string(message.Content))
		t.FailNow()
	}
}

// Test if the message expires
// fails if there is an failure in delivering the message
// fails if there is an failure in retrieving the message
// fails if IsExpired returns false
func TestNotAck(t *testing.T) {
	t.Parallel()
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new Consumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	message, ok := receiveMessage(consumer)
	if ok {
		time.Sleep(messages.DeliveryTimeout + (time.Millisecond * 100))
		expired := message.IsExpired()
		if !expired {
			t.Error("message did not expire")
			t.Log(message.DeliveryErr())
			t.FailNow()
		}
	}
}

// Test if an expired message is re-delivered
// fails if there is an failure in delivering the message
// fails if there is an failure in retrieving the message
// fails if initial message IsExpired returns false

func TestReAcquireMessage(t *testing.T) {
	t.Parallel()
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new Consumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	message, ok := receiveMessage(consumer)
	if ok {
		// delay to make the message expire
		time.Sleep(messages.DeliveryTimeout + (time.Millisecond * 100))
		if !message.IsExpired() {
			t.Error("message did not expire")
			t.Error(message.DeliveryErr())
			t.Error(message.GetId())
			t.FailNow()
		}
	}
	t.Log("message has expired trying to re-acquire")
	id := message.GetId()

	// delay for the route expired messages worker to do its job
	time.Sleep(time.Millisecond * 100)
	// acquire previous expired message as an "new" message
	message, ok = receiveMessage(consumer)
	if ok {
		// checks if new acquired message is the same as the expired one
		if message.GetId() != id {
			t.Errorf("message id mismatch expected %v got %v", id, message.GetId())
			t.FailNow()
		}
	}
}

// Test getting an message that has expired by other consumer
// fails if there is an failure in delivering the message
// fails if there is an failure in retrieving the message
// fails if there is no message on the waiting map on the test Route
// fails if the first consumer is not the one responsible to that message in the waiting delivery map in the Router
// fails if the expired message acquired by the second consumer has different id
// fails if the second consumer is not the one responsible to that message in the waiting delivery map in the Router
// fails if the message is not expired after expiring delay
func TestAcquireExpiredMessageByOtherConsumer(t *testing.T) {
	t.Parallel()
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// create consumer that will let the message expire
	consumer1 := router.GetConsumer("test")
	// create consumer that will retrieve the expired message as new
	consumer2 := router.GetConsumer("test")

	route, ok := router.GetRoute("test")
	if !ok {
		t.Error("failed getting route")
		t.FailNow()
	}

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok = publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	message, ok := receiveMessage(consumer1)
	// gets the original message id of the expiring message
	originalMessageId := message.GetId()
	if !ok {
		t.Error("failed getting message")
		t.FailNow()
	}
	// gets the consumer id attached to the waiting delivery map in the Router
	// this should change if the message is acquired by other consumer
	routeConsumerId, ok := route.GetConsumerId(message)
	if !ok {
		t.Error("failed getting router consumer id for message")
		t.FailNow()
	}
	// fails if the first consumer is not the one responsible to that message in the waiting delivery map in the Router
	if routeConsumerId != consumer1.GetId() {
		t.Errorf("route consumer id mismatch expected %v got %v", consumer1.GetId(), routeConsumerId)
		t.FailNow()
	}
	// delay to let the message expire
	time.Sleep(messages.DeliveryTimeout + (time.Millisecond * 100))
	if !message.IsExpired() {
		t.Error("message is not expired")
		t.FailNow()
	}
	// retrieve expired message as new using consumer 2
	var message2 *messages.RouterMessage
	message2, ok = receiveMessage(consumer2)
	if !ok {
		t.FailNow()
	}

	if message2.GetId() != originalMessageId {
		t.Errorf("message id mismatch expected %v got %v", originalMessageId, message2.GetId())
	}

	routeConsumerId2, _ := route.GetConsumerId(message2)
	if routeConsumerId2 != consumer2.GetId() {
		t.Errorf("route consumer id mismatch expected %v got %v", consumer2.GetId(), routeConsumerId2)
		t.FailNow()
	}

}

// Test creating more than one publisher and more than one route
// fails if any publisher fails in delivering the message
// fails if any published routes is empty
func TestCreateMultiplePublishers(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test routes
	router.RegisterRoute("test1", tinyQ.DefaultMaxRouteSize)
	router.RegisterRoute("test2", tinyQ.DefaultMaxRouteSize)
	router.RegisterRoute("test3", tinyQ.DefaultMaxRouteSize)

	// creating the publishers
	publisher1 := router.GetPublisher()
	publisher2 := router.GetPublisher()
	publisher3 := router.GetPublisher()

	var ok bool
	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok = publisher1.Publish([]byte(messageText), "test1")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// publishes the second test message
	_, ok = publisher2.Publish([]byte(messageText), "test2")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}

	// creates the test message and publishes

	_, ok = publisher3.Publish([]byte(messageText), "test3")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}
	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	// check on route 1 if there is an message
	route1, ok := router.GetRoute("test1")
	if !ok {
		t.Error("failed getting route 1")
		t.FailNow()
	}
	if route1.Size() == 0 {
		t.Error("failed route 1 have 0 messages")
		t.FailNow()
	}

	// check on route 2 if there is an message
	route2, ok2 := router.GetRoute("test1")
	if !ok2 {
		t.Error("failed getting route 2")
		t.FailNow()
	}
	if route2.Size() == 0 {
		t.Error("failed route 2 have 0 messages")
		t.FailNow()
	}

	// check on route 3 if there is an message
	route3, ok3 := router.GetRoute("test1")
	if !ok3 {
		t.Error("failed getting route 3")
		t.FailNow()
	}
	if route3.Size() == 0 {
		t.Error("failed route 3 have 0 messages")
		t.FailNow()
	}
}

// Test sending messages from multiple go routines
// fails if the publisher workers does not finish sending each 10 messages in 5 seconds
// fails if the message count in the route is not 30
func TestMultiSend(t *testing.T) {
	t.Parallel()
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	returnChannel := make(chan int)
	// start 3 go routines "publisher workers" sending 10 messages each
	// each worker sends an true to the return channel to track the progress
	for worker := range 3 {
		// waitGroup.Add(1)
		go func() {
			t.Logf("worker %v is online", worker)
			publisher := router.GetPublisher()
			for i := range 10 {
				// creates individual message
				msg := Msg{
					WorkerId:  worker,
					MessageId: i,
				}
				messageText, _ := json.Marshal(msg)
				// creates the test message and publishes
				_, ok := publisher.Publish([]byte(messageText), "test")

				if !ok {
					t.Errorf("failed delivering message to Router from worker %v", worker)
				}
			}
			returnChannel <- worker
			// waitGroup.Done()
		}()
	}

	t.Log("workers started waiting them to finish")
	// workers publish timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	done := 0
waitLoop:
	for {
		select {
		case <-ctx.Done():
			cancel()
			t.Error("timeout waiting workers")
			t.FailNow()
			return
		case workerId := <-returnChannel:
			t.Logf("worker %v is done", workerId)
			done++
			if done >= 3 {
				cancel()
				t.Log("workers finished publishing messages collecting")
				break waitLoop
			}
		}
	}

	route, ok := router.GetRoute("test")
	if !ok {
		t.Error("failed getting route")
		t.FailNow()
	}
	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	if route.Size() != 30 {
		t.Errorf("route have %v messages expected %v", route.Size(), 30)
		t.FailNow()
	}
}

// Test multiple go routines consumers
// fails if there is an failure in delivering the messages
// fails if test route does not have 20 messages before the retrieval process
// fails if retrieved messages channel does not have 20 messages
// fails if the sum of every message (1..20) is not 210 (the sum of every number between 1 and 20 is 210)
func TestMultiReceive(t *testing.T) {
	t.Parallel()
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	for i := range 20 {
		// creates the test message and publishes
		// each message is 1..20 in byte array
		arr := make([]byte, 4)
		binary.BigEndian.PutUint32(arr[0:4], uint32(i+1))
		_, ok := publisher.Publish(arr, "test")

		if !ok {
			t.Error("failed delivering message to Router")
			t.FailNow()
		}
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	route, _ := router.GetRoute("test")
	if route.Size() != 20 {
		t.Errorf("test failed route has %v expected %v", route.Size(), 20)
		t.FailNow()
		return
	}
	t.Log("messages sent, processing output")

	// creating the output channel for the workers responses
	// 100 should be plenty on any mistake case
	returnChan := make(chan []byte, 100)

	// consume timer to give plenty of time to exhaust the 20 messages
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	// starting context to initiate all consumer routines at the same time
	startCtx, start := context.WithCancel(context.Background())
	for workerId := range 3 {
		go func() {
			t.Logf("worker %v is online\n", workerId)
			consumer := router.GetConsumer("test")
			<-startCtx.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					message, ok := receiveMessage(consumer)
					if ok {
						consumer.Ack(message)
						returnChan <- message.Content
					}
				}
			}

		}()
	}

	// delay to make sure every worker is set
	time.Sleep(time.Millisecond * 100)
	t.Log("workers are set")

	route, _ = router.GetRoute("test")

	// this size checking is safe due to no consumer is running
	t.Logf("route has %v messages", route.Size())

	// start the consumers
	start()

	// blocks until the consume timer runs out
	<-ctx.Done()

	t.Log("time finished processing results")
	if len(returnChan) != 20 {
		t.Errorf("test failed return channel have %v expected %v", len(returnChan), 20)
		t.FailNow()
		return
	}
	// validates if no message is missing using sum of 1..20 = 210
	t.Log("checking responses")
	var num uint32
	num = 0
	for range 20 {
		content := <-returnChan
		num += binary.BigEndian.Uint32(content)
	}
	if num != 210 {
		t.Errorf("test failed sum of messages is %v expected %v", num, 210)
		t.FailNow()
	}
}

// Test subscriber calling an call back function
// fails if there is an failure in delivering the messages
// fails if subscriber is not running
// fails if subscriber does not call the callback function in 10 seconds
// fails if the callback function is not executed
// fails if the id of the message received by the callback function does not match original message id
func TestSubscriber(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	// create an new publisher
	publisher := router.GetPublisher()

	// creates the test message and publishes
	const messageText string = "this is a test"
	messageId, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed delivering message to Router")
		t.FailNow()
	}
	// timer to call callback function
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	// cancel if message received in callback function does not match original message
	failCtx, failCancel := context.WithCancel(context.Background())

	// using channel to bypass scope and allow response from callback function
	messageIdChan := make(chan string, 1)
	messageIdChan <- messageId

	// callback function in compliance to subscriber.CallBack (*messages.RouterMessage,contextCancelFunc)
	testCallback := func(message *messages.RouterMessage, ack context.CancelFunc) {
		id := <-messageIdChan
		// log.Printf("want %v got %v", id, message.GetId())
		// log.Print(id != message.GetId())
		if id != message.GetId() {
			log.Printf("message id expected %v \n", messageId)
			failCancel()
		}
		cancel()
		ack()
	}
	t.Log("creating subscriber")
	subscriber, ok := router.GetSubscriber("test", testCallback)

	if !ok {
		t.Error("failed creating subscriber")
		t.FailNow()
	}

	if subscriber.IsRunning() == false {
		t.Error("subscriber is not running")
		t.FailNow()
	}

	<-ctx.Done()

	if errors.Is(failCtx.Err(), context.Canceled) == true {
		t.Error("message id mismatch")
		t.FailNow()
	}
}

// Test Router odometer
// fails if the Total messages count is not 20
// fails if the Total messages delivered count is not 10
// fails if the Total messages failed count is not 10
func TestOdometer(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)
	router.EnableTelemetry()
	publisher := router.GetPublisher()

	for range 10 {
		publisher.Publish([]byte("test"), "test")
	}

	for range 10 {
		publisher.Publish([]byte("test"), "Non exists")
	}

	time.Sleep(time.Second * 2)

	telemetry := router.GetTelemetry()

	if telemetry.TotalMessages != 20 {
		t.Errorf("failed counting total messages expected %v got %v\n", 20, telemetry.TotalMessages)
		t.FailNow()
	}

	if telemetry.TotalDelivered != 10 {
		t.Errorf("failed counting total messages delivered expected %v got %v\n", 10, telemetry.TotalDelivered)
		t.FailNow()
	}

	if telemetry.TotalLost != 10 {
		t.Errorf("failed counting total messages lost expected %v got %v\n", 10, telemetry.TotalLost)
		t.FailNow()
	}

}

// Test if an Route is closed other routes keep working and the consumers attached to that route also stops
// Fails if the ClosingRoute is not closed
// Fails if the consumer attached to the ClosingRoute is not closed after ClosingRoute close
// Fails if the KeepRunningRoute does not have 2 message after closing the ClosingRoute
// Fails if the KeepRunningRoute is closed after ClosingRoute is closed
// Fails if the KeepRunningConsumer is closed after ClosingRoute is closed
func TesStopRoute(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	testClosingRoute := "test1"
	testKeepRunningRoute := "test2"

	router.RegisterRoute(testClosingRoute, tinyQ.DefaultMaxRouteSize)
	router.RegisterRoute(testKeepRunningRoute, tinyQ.DefaultMaxRouteSize)

	publisher := router.GetPublisher()

	// consumer that wil stop working
	ClosingConsumer := router.GetConsumer(testClosingRoute)
	// consumer that should keep running
	KeepRunningConsumer := router.GetConsumer(testKeepRunningRoute)

	publisher.Publish([]byte("test"), testClosingRoute)
	publisher.Publish([]byte("test"), testKeepRunningRoute)

	// route that wil be closed
	ClosingRoute, _ := router.GetRoute(testClosingRoute)
	// route that should keep working
	KeepRunningRoute, _ := router.GetRoute(testKeepRunningRoute)

	ClosingRoute.CloseRoute()

	if !ClosingRoute.IsClosed() {
		t.Error("test failed route is not closed")
		t.FailNow()
	}

	if !ClosingConsumer.IsClosed() {
		t.Error("test failed consumer is not closed after route close")
		t.FailNow()
	}

	publisher.Publish([]byte("test"), testKeepRunningRoute)
	time.Sleep(time.Millisecond * 10)
	if KeepRunningRoute.Size() != 2 {
		t.Errorf("test failed route 2 size mismatch expected %v got %v \n", 2, KeepRunningRoute.Size())
		t.FailNow()
	}

	if KeepRunningRoute.IsClosed() {
		t.Error("test failed route 2 is closed")
		t.FailNow()
	}

	if KeepRunningConsumer.IsClosed() {
		t.Error("test failed consumer2 is closed")
		t.FailNow()
	}
}

func TestClosingRouter(t *testing.T) {
	t.Log("starting router")
	router := startRouter()

	// register the test route
	router.RegisterRoute("test1", tinyQ.DefaultMaxRouteSize)
	router.RegisterRoute("test2", tinyQ.DefaultMaxRouteSize)

	publisher := router.GetPublisher()

	dedicatedPublisher := router.GetDedicatedPublisher("test1")

	consumer1 := router.GetConsumer("test1")

	testCallback := func(message *messages.RouterMessage, ack context.CancelFunc) {
		ack()
	}

	subscriber, _ := router.GetSubscriber("test2", testCallback)

	router.StopRouter()
	time.Sleep(time.Millisecond * 100)

	if router.IsRunning() {
		t.Error("test failed router is not closed")
		t.FailNow()
	}

	if !publisher.IsClosed() {
		t.Error("test failed publisher is not closed")
		t.FailNow()
	}

	if !dedicatedPublisher.IsClosed() {
		t.Error("test failed dedicated publisher is not closed")
		t.FailNow()
	}

	if !consumer1.IsClosed() {
		t.Error("test failed consumer is not closed")
		t.FailNow()
	}

	if !subscriber.IsClosed() {
		t.Error("test failed subscriber is not closed")
		t.FailNow()
	}

}

// Test Dedicated publisher
// fails if route does not exists
// fails if there is an failure in delivering the messages
// fails if the route size is not 1 after message delivery
func TestDedicatedPublisher(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test", tinyQ.DefaultMaxRouteSize)

	dedicatedPublisher := router.GetDedicatedPublisher("test")

	route, ok := router.GetRoute("test")
	if !ok {
		t.Error("failed getting route")
		t.FailNow()
	}

	_, ok = dedicatedPublisher.Publish([]byte("test"))

	if !ok {
		t.Error("failed publishing message")
		t.FailNow()
	}
	if route.Size() != 1 {
		t.Errorf("test failed route size mismatch expected %v got %v ", 1, route.Size())
		t.FailNow()
	}
}

func TestHooksEnable(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := tinyQ.Router{}
	err := router.EnableHooks()
	if err != nil {
		t.Errorf("test failed enabling hooks returned error %v \n", err)
	}
	router.InitRouter()
}

func TestHooksEnableAfterInit(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := tinyQ.Router{}
	router.InitRouter()
	err := router.EnableHooks()
	if err == nil {
		t.Error("test failed EnableHooks should return error on call after init, returned nil")
		t.FailNow()
	}
}

func TestPostAckHook(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := tinyQ.Router{}
	t.Log("enabling hooks")
	router.EnableHooks()
	router.InitRouter()
	t.Log("router start")
	hookReturnChannel := make(chan *messages.RouterMessage, 1)
	var hook1 hooks.Hook
	testRouteKey := "testRoute"
	hook1 = func(message *messages.RouterMessage) error {
		hookReturnChannel <- message
		return nil
	}
	router.RegisterRoute(testRouteKey, tinyQ.DefaultMaxRouteSize)
	err := router.AddPostAckHook(testRouteKey, hook1)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	publisher := router.GetPublisher()
	consumer := router.GetConsumer(testRouteKey)
	route, _ := router.GetRoute(testRouteKey)
	if route.HooksCount() != 1 {
		t.Errorf("test failed route hooks count miss match wanted %v got %v", 1, route.HooksCount())
		t.FailNow()
	}
	testMessage := []byte("test")
	publisher.Publish(testMessage, testRouteKey)
	msg, ok := receiveMessage(consumer)
	if ok {
		consumer.Ack(msg)
		time.Sleep(time.Millisecond * 500)
		if len(hookReturnChannel) != 1 {
			t.Errorf("test failed return channel length miss match wanted %v got %v\n", 1, len(hookReturnChannel))
			t.FailNow()
		}
		if string(msg.Content) == string(testMessage) {
			hookMsg := <-hookReturnChannel
			if string(hookMsg.Content) != string(testMessage) {
				t.Errorf("test failed message content from return channel wanted %v got %v\n", string(testMessage), string(hookMsg.Content))
				t.FailNow()
			}
		}
	}
}

func TestPostAckHookUnregisteredRoute(t *testing.T) {
	t.Parallel()
	router := tinyQ.Router{}
	router.EnableHooks()
	router.InitRouter()
	defer router.StopRouter()
	hook := func(message *messages.RouterMessage) error {
		t.Log("executing hook")
		return nil
	}
	err := router.AddPostAckHook("test", hook)
	if err == nil {
		t.Error("test failed error is nil")
		t.FailNow()
	}

}

func TestPrePostHook(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := tinyQ.Router{}
	t.Log("enabling hooks")
	router.EnableHooks()
	router.InitRouter()
	t.Log("router start")
	hookReturnChannel := make(chan *messages.RouterMessage, 1)
	var hook1 hooks.Hook
	testRouteKey := "testRoute"
	hook1 = func(message *messages.RouterMessage) error {
		t.Log("executing hook")
		hookReturnChannel <- message
		return nil
	}
	router.RegisterRoute(testRouteKey, tinyQ.DefaultMaxRouteSize)
	err := router.AddMessagePostInHook(testRouteKey, hook1)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	publisher := router.GetPublisher()
	consumer := router.GetConsumer(testRouteKey)
	testMessage := []byte("test")
	publisher.Publish(testMessage, testRouteKey)
	msg, ok := receiveMessage(consumer)
	if ok {
		consumer.Ack(msg)
		time.Sleep(time.Millisecond * 500)
		if len(hookReturnChannel) != 1 {
			t.Errorf("test failed return channel length miss match wanted %v got %v\n", 1, len(hookReturnChannel))
			t.FailNow()
		}
		if string(msg.Content) == string(testMessage) {
			hookMsg := <-hookReturnChannel
			if string(hookMsg.Content) != string(testMessage) {
				t.Errorf("test failed message content from return channel wanted %v got %v\n", string(testMessage), string(hookMsg.Content))
				t.FailNow()
			}
		}
	}
}

func TestPrePostHookUnregisteredRoute(t *testing.T) {
	t.Parallel()
	router := tinyQ.Router{}
	router.EnableHooks()
	router.InitRouter()
	defer router.StopRouter()
	hook := func(message *messages.RouterMessage) error {
		t.Log("executing hook")
		return nil
	}
	err := router.AddMessagePostInHook("test", hook)
	if err == nil {
		t.Error("test failed error is nil")
		t.FailNow()
	}
}

func TestUnregisterRoute(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := startRouter()
	testRoute := "test"
	router.RegisterRoute(testRoute, tinyQ.DefaultMaxRouteSize)
	router.UnregisterRoute(testRoute)
	if router.HasRoute(testRoute) {
		t.Error("test failed router still has route")
		t.FailNow()
	}
}

func TestUnregisterRouteWithHooks(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := tinyQ.Router{}
	router.EnableHooks()
	router.InitRouter()
	testRoute := "test"
	router.RegisterRoute(testRoute, tinyQ.DefaultMaxRouteSize)
	router.UnregisterRoute(testRoute)
	if router.HasRoute(testRoute) {
		t.Error("test failed router still has route")
		t.FailNow()
	}
}

func TestStopRoute(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := startRouter()
	testRoute := "test"
	router.RegisterRoute(testRoute, tinyQ.DefaultMaxRouteSize)
	route, _ := router.GetRoute(testRoute)
	router.StopRoute(testRoute)
	if !route.IsClosed() {
		t.Error("test failed route is still running")
		t.FailNow()
	}
}

func TestStopRouteWithHooks(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := tinyQ.Router{}
	router.EnableHooks()
	router.InitRouter()
	testRoute := "test"
	router.RegisterRoute(testRoute, tinyQ.DefaultMaxRouteSize)
	route, _ := router.GetRoute(testRoute)
	router.StopRoute(testRoute)
	if !route.IsClosed() {
		t.Error("test failed route is still running")
		t.FailNow()
	}
}

func TestStopRouteNotRegistered(t *testing.T) {
	t.Parallel()
	t.Log("starting router")
	router := startRouter()
	testRoute := "test"
	resp := router.StopRoute(testRoute)
	if resp {
		t.Error("test failed route stop should return false to unregistered routes")
	}
}

// func TestRouteOverwhelm(t *testing.T) {
// 	t.Log("starting router")
// 	router := startRouter()
// 	defer router.StopRouter()
// 	router.EnableTelemetry()
// 	router.RegisterRoute("test", 5)
// 	publisher := router.GetPublisher()
// 	stop, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
// 	defer cancel()
// 	go func() {
// 	feedLoop:
// 		for {
// 			select {
// 			case <-stop.Done():
// 				break feedLoop
// 			default:
// 				t.Log("published")
// 				publisher.Publish([]byte("msg"), "test")
// 			}
// 		}
// 	}()
// 	<-stop.Done()
// 	time.Sleep(time.Second * 100)
// 	telemetry := router.GetTelemetry()
// 	if telemetry.TotalLost == 0 {
// 		t.Errorf("test failed total messages lost mismatch expected %v got %v \n", "!0", telemetry.TotalLost)
// 		t.FailNow()
// 	}
// }
