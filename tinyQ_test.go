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
	"github.com/Ingo-Braun/TinyQ/messages"
)

// starts a new router
func startRouter() *tinyQ.Router {
	router := tinyQ.Router{}
	router.InitRouter()
	return &router
}

// helper function to get messages from an consumer
// consumer shuld timeout before
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
	router.RegisterRoute("test")
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
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	if publisher == nil {
		t.Error("failed to create Publisher")
		t.FailNow()
	}
}

// Tests creating an consumer
// fails if the consumer is nil
func TestCreateBasicConsummer(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	consummer := router.GetConsumer("test")
	if consummer == nil {
		t.Error("failed to create Consummer")
		t.FailNow()
	}
}

// Tests if creating an conssumer to an not registered route automaticaly creates the route
// fails if the consumer is nil
// fails if the router dont create the route automaticaly
func TestCreateBasicConssumerNotRegisteredRoute(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	consummer := router.GetConsumer("test")
	if consummer == nil {
		t.Error("failed to create Consummer")
		t.FailNow()
	}
	if router.HasRoute("test") {
		t.Log("router created test route automaticaly")
	}
}

// Test sending an message
// fails if there is an failure in delivering the message
// fails if the route size is 0
// the reading of the Route size is realiable due to not having an consummer attatched to it
func TestSend(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// get the Route struct
	route, ok := router.GetRoute("test")

	if ok {
		// delay to wait for the Router delivery worker
		// to pass the message from the input channel to the destination Route
		time.Sleep(time.Millisecond * 100)

		if route.Size() == 0 {
			t.Logf("route didint receive message queue size %v", route.Size())
			t.FailNow()
		}
	}
	t.Logf("route received message !, queue size %v", route.Size())
}

// Tests sending an message and receiving using an consumer
// fails if there is an failure in delivering the message
// fails if there is an failure in retriving the message
// fails if the message content is diferent from the original
// fails if the message id is diferent from the id returned on publish
func TestSendAndReceive(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new conssumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	messageId, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	// get the Route struct
	route, _ := router.GetRoute("test")

	// logs the queue size
	// this reading shuld be reliable since no message has been consumed from the Route
	t.Logf("queue size %v", route.Size())

	// retrives the message
	message, ok := receiveMessage(consumer)
	if !ok {
		t.Error("failed retriving the message")
		t.FailNow()
	}
	if string(message.Content) != messageText {
		t.Errorf("failed receiving message sent %v received %v", messageText, string(message.Content))
		t.FailNow()
	}
	if message.GetId() != messageId {
		t.Errorf("message id missmatch expected %v got %v", messageId, message.GetId())
		t.FailNow()
	}
}

// Tests message Ack
// fails if there is an failure in delivering the message
// fails if there is an failure in retriving the message
// fails if Ack returns not nil
func TestAck(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new conssumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	message, ok := receiveMessage(consumer)
	if !ok {
		t.Error("failed retriving the message")
		t.FailNow()
	}
	if ok && string(message.Content) == messageText {
		t.Log("testing ack")
		t.Logf("is message expired %v", message.IsExpired())
		t.Logf("is message valide %v", message.IsValid())
		if message.IsExpired() == false && message.IsValid() {
			err := consumer.Ack(message)
			if !err {
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
// fails if there is an failure in retriving the message
// fails if IsExpired returns false
func TestNotAck(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new conssumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	message, ok := receiveMessage(consumer)
	if ok {
		time.Sleep(time.Millisecond * (messages.DeliveryTimeout + 100))
		expired := message.IsExpired()
		if !expired {
			t.Error("message didint expire")
			t.Log(message.DeliveryErr())
			t.FailNow()
		}
	}
}

// Test if an expired message is re-delivered
// fails if there is an failure in delivering the message
// fails if there is an failure in retriving the message
// fails if initial message IsExpired returns false

func TestReAquireMessage(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// create an new conssumer to the test route
	consumer := router.GetConsumer("test")

	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// delay to wait for the Router delivery worker
	// to pass the message from the input channel to the destination Route
	time.Sleep(time.Millisecond * 100)

	message, ok := receiveMessage(consumer)
	if ok {
		// deley to make the message expire
		time.Sleep(time.Millisecond * (messages.DeliveryTimeout + 200))
		if !message.IsExpired() {
			t.Error("message didint expire")
			t.Error(message.DeliveryErr())
			t.Error(message.GetId())
			t.FailNow()
		}
	}
	t.Log("message has expired trying to re-aquire")
	id := message.GetId()

	// delay for the route expired messages worker to do its job
	time.Sleep(time.Millisecond * 100)
	// acquire previus expired message as an "new" message
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
// fails if there is an failure in retriving the message
// fails if there is no message on the wating map on the test Route
// fails if the first consumer is not the one responsible to that message in the wating delivery map in the Router
// fails if the expired message acquired by the second consumer has diferent id
// fails if the second consumer is not the one responsible to that message in the wating delivery map in the Router
// fails if the message is not expired after expiring delay
func TestAquireExpiredMessageByOtherConsumer(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// create consumer that will let the message expire
	consumer1 := router.GetConsumer("test")
	// create consumer that will retreive the expired message as new
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
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	message, ok := receiveMessage(consumer1)
	// gets the original message id of the expiring message
	originalMessageId := message.GetId()
	if !ok {
		t.Error("failed getting message")
		t.FailNow()
	}
	// gets the consumer id attatched to the waiting delivery map in the Router
	// this shuld change if the message is acquired by other consumer
	routeConsumerId, ok := route.GetConsummerId(message)
	if !ok {
		t.Error("failed getting router consumer id for message")
		t.FailNow()
	}
	// fails if the first consumer is not the one responsible to that message in the wating delivery map in the Router
	if routeConsumerId != consumer1.GetId() {
		t.Errorf("route consumer id mismatch expected %v got %v", consumer1.GetId(), routeConsumerId)
		t.FailNow()
	}
	// delay to let the message expire
	time.Sleep(time.Millisecond * (messages.DeliveryTimeout + 100))
	if !message.IsExpired() {
		t.Error("message is not expired")
		t.FailNow()
	}
	// retrive expired message as new using consumer 2
	var message2 *messages.RouterMessage
	message2, ok = receiveMessage(consumer2)
	if !ok {
		t.FailNow()
	}

	if message2.GetId() != originalMessageId {
		t.Errorf("message id mismatch expected %v got %v", originalMessageId, message2.GetId())
	}

	routeConsumerId2, _ := route.GetConsummerId(message2)
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
	router.RegisterRoute("test1")
	router.RegisterRoute("test2")
	router.RegisterRoute("test3")

	// creating the publishers
	publisher1 := router.GetPublisher()
	publisher2 := router.GetPublisher()
	publisher3 := router.GetPublisher()

	var ok bool
	// creates the test message and publishes
	const messageText string = "this is a test"
	_, ok = publisher1.Publish([]byte(messageText), "test1")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// publishes the second test message
	_, ok = publisher2.Publish([]byte(messageText), "test2")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}

	// creates the test message and publishes

	_, ok = publisher3.Publish([]byte(messageText), "test3")

	if !ok {
		t.Error("failed deliveing message to Router")
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
// fails if the publisher workers dont finish sending each 10 messages in 5 seconds
// fails if the message count in the route is not 30
func TestMultiSend(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	returnChannel := make(chan int)
	// start 3 go routines "publisher workers" sending 10 messages each
	// each worker sends an true to the reutn channel to track the progress
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
					t.Errorf("failed deliveing message to Router from worker %v", worker)
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
			t.Error("timedout wating workers")
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
// fails if test route dont have 20 messages before the retrival process
// fails if retreived messages channel dont have 20 messages
// fails if the sum of every message (1..20) is not 210 (the sum of every number between 1 and 20 is 210)
func TestMultiReceive(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	for i := range 20 {
		// creates the test message and publishes
		// each message is 1..20 in byte array
		arr := make([]byte, 4)
		binary.BigEndian.PutUint32(arr[0:4], uint32(i+1))
		_, ok := publisher.Publish(arr, "test")

		if !ok {
			t.Error("failed deliveing message to Router")
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
	// 100 shuld be plenty on any mistake case
	returnChan := make(chan []byte, 100)

	// consume timer to give plenty of time to exaust the 20 messages
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	// starting contex to initiate all consumer routines at the same time
	startCtx, start := context.WithCancel(context.Background())
	for workerId := range 3 {
		go func() {
			t.Logf("worker %v is online", workerId)
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

	// delay to make schure every worker is set
	time.Sleep(time.Millisecond * 100)
	t.Log("workers are set")

	route, _ = router.GetRoute("test")

	// this size checking is safe due to no consumer is running
	t.Logf("route has %v messages", route.Size())

	// start the consummers
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
// fails if subscriber dont call the callback function in 10 seconds
// fails if the callback function is not executed
// fails if the id of the message received by the callback function dosent match original message id
func TestSubscriber(t *testing.T) {
	// setup
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)

	// register the test route
	router.RegisterRoute("test")

	// create an new publisher
	publisher := router.GetPublisher()

	// creates the test message and publishes
	const messageText string = "this is a test"
	messageId, ok := publisher.Publish([]byte(messageText), "test")

	if !ok {
		t.Error("failed deliveing message to Router")
		t.FailNow()
	}
	// timer to call callback function
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	// cancel if message received in callback function dosent match original message
	failCtx, failCancel := context.WithCancel(context.Background())

	// using channel to bypass scope and allow response from callback function
	messageIdChan := make(chan string, 1)
	messageIdChan <- messageId

	// callback function in compliance to subscriber.CallBack (*messages.RouterMessage,contextCancelFunc)
	testCallback := func(message *messages.RouterMessage, ack context.CancelFunc) {
		id := <-messageIdChan
		log.Printf("want %v got %v", id, message.GetId())
		log.Print(id != message.GetId())
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
