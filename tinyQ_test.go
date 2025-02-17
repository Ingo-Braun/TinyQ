package tinyQ_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	tinyQ "github.com/Ingo-Braun/TinyQ"
	"github.com/Ingo-Braun/TinyQ/consumer"
	"github.com/Ingo-Braun/TinyQ/structs/messages"
)

func startRouter() *tinyQ.Router {
	router := tinyQ.Router{}
	router.InitRouter()
	return &router
}

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

type Msg struct {
	WorkerId  int `json:"worker_id"`
	MessageId int `json:"message_id"`
}

func TestCreateRouter(t *testing.T) {
	t.Log("starting router")
	router := tinyQ.Router{}
	router.InitRouter()
	t.Log("stopping router")
	router.StopRouter()
}

func TestCreateRoute(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	t.Log("creating route test")
	channel := router.RegisterRoute("test")
	if channel == nil {
		t.Error("failed to create route")
	}
}

func TestCreatePublisher(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	if publisher == nil {
		t.Error("failed to create Publisher")
	}
}

func TestCreateBasicConsummer(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	consummer := router.GetConsumer("test")
	if consummer == nil {
		t.Error("failed to create Consummer")
		t.Fail()
	}
	if !router.HasRoute("test") {
		t.Error("router failed registering test route")
	}
}

func TestCreateBasicCOnssumerNotRegisteredRoute(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	consummer := router.GetConsumer("test")
	if consummer == nil {
		t.Error("failed to create Consummer")
		t.Fail()
	}
	if router.HasRoute("test") {
		t.Log("router created test route automaticaly")
	}
}

func TestSend(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	const messageText string = "this is a test"
	publisher.Publish([]byte(messageText), "test")
	route, ok := router.GetRoute("test")
	time.Sleep(time.Millisecond * 100)
	if ok {
		if route.Size() == 0 {
			t.Logf("route didint receive message queue size %v", route.Size())
			t.Fail()
		}
	}
	t.Logf("route received message !, queue size %v", route.Size())
}

func TestSendAndReceive(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	consumer := router.GetConsumer("test")
	const messageText string = "this is a test"
	publisher.Publish([]byte(messageText), "test")
	time.Sleep(time.Millisecond * 100)
	route, _ := router.GetRoute("test")
	t.Logf("queue size %v", route.Size())

	message, ok := receiveMessage(consumer)
	message.Ack()
	if ok && string(message.Content) != messageText {
		t.Errorf("failed receiving message sent %v received %v", messageText, string(message.Content))
		t.Fail()
	}

}

func TestAck(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	consumer := router.GetConsumer("test")
	const messageText string = "this is a test"
	publisher.Publish([]byte(messageText), "test")
	time.Sleep(time.Millisecond * 100)
	message, ok := receiveMessage(consumer)
	if ok && string(message.Content) == messageText {
		t.Log("testing ack")
		t.Logf("is message expired %v", message.IsExpired())
		t.Logf("is message valide %v", message.IsValid())
		if message.IsExpired() == false && message.IsValid() {
			err := message.Ack()
			if !err {
				t.Fail()
			}
		}
	} else if ok && string(message.Content) != messageText {
		t.Errorf("failed receiving message sent %v received %v", messageText, string(message.Content))
	}
}

func TestNotAck(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	consumer := router.GetConsumer("test")
	const messageText string = "this is a test"
	publisher.Publish([]byte(messageText), "test")
	time.Sleep(time.Millisecond * 100)
	message, ok := receiveMessage(consumer)
	if ok {
		time.Sleep(time.Millisecond * (messages.DeliveryTimeout + 100))
		expired := message.IsExpired()
		if !expired {
			t.Error("message didint expire")
			t.Log(message.DeliveryErr())
			t.Fail()
		}
	}
}

func TestReAquireMessage(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	consumer := router.GetConsumer("test")
	const messageText string = "this is a test"
	publisher.Publish([]byte(messageText), "test")
	time.Sleep(time.Millisecond * 100)
	message, ok := receiveMessage(consumer)
	if ok {
		time.Sleep(time.Millisecond * (messages.DeliveryTimeout + 200))
		if !message.IsExpired() {
			t.Error("message didint expire")
			t.Error(message.DeliveryErr())
			t.Error(message.GetId())
			t.Fail()
			return
		}
	}
	t.Log("message has expired trying to re-aquire")
	id := message.GetId()
	time.Sleep(time.Millisecond * 100)
	message, ok = receiveMessage(consumer)
	if ok {
		message.Ack()
		if message.GetId() != id {
			t.Errorf("message id mismatch expected %v got %v", id, message.GetId())
			t.Fail()
		}
	}
}

func TestCreateMultiplePublishers(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test1")
	router.RegisterRoute("test2")
	router.RegisterRoute("test3")
	publisher1 := router.GetPublisher()
	publisher1.Publish([]byte("test1"), "test1")
	publisher2 := router.GetPublisher()
	publisher2.Publish([]byte("test1"), "test2")
	publisher3 := router.GetPublisher()
	publisher3.Publish([]byte("test1"), "test3")
	time.Sleep(time.Millisecond * 100)
	route1, ok := router.GetRoute("test1")
	if !ok {
		t.Error("failed getting route 1")
		t.Fail()
	}
	if route1.Size() == 0 {
		t.Error("failed route 1 have 0 messages")
		t.Fail()
	}

	route2, ok2 := router.GetRoute("test1")
	if !ok2 {
		t.Error("failed getting route 2")
		t.Fail()
	}
	if route2.Size() == 0 {
		t.Error("failed route 2 have 0 messages")
		t.Fail()
	}

	route3, ok3 := router.GetRoute("test1")
	if !ok3 {
		t.Error("failed getting route 3")
		t.Fail()
	}
	if route3.Size() == 0 {
		t.Error("failed route 3 have 0 messages")
		t.Fail()
	}
}

func TestMultiSend(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	// waitGroup := sync.WaitGroup{}
	returnChannel := make(chan int)
	for worker := range 3 {
		// waitGroup.Add(1)
		go func() {
			t.Logf("worker %v is online", worker)
			publisher := router.GetPublisher()
			for i := range 10 {
				msg := Msg{
					WorkerId:  worker,
					MessageId: i,
				}
				data, _ := json.Marshal(msg)
				publisher.Publish([]byte(data), "test")
			}
			returnChannel <- worker
			// waitGroup.Done()
		}()
	}
	t.Log("workers started waiting them to finish")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	done := 0
waitLoop:
	for {
		select {
		case <-ctx.Done():
			cancel()
			t.Error("timedout wating workers")
			t.Fail()
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
		t.Fail()
	}
	if route.Size() != 30 {
		t.Errorf("route have %v messages expected %v", route.Size(), 30)
		t.Fail()
	}
}

// func TestMultipleConsumers(t *testing.T) {
// 	t.Log("starting router")
// 	router := startRouter()
// 	t.Cleanup(router.StopRouter)
// 	router.RegisterRoute("test")
// 	consumer1 := router.GetConsumer("test")
// 	consumer2 := router.GetConsumer("test")

// 	if reflect.DeepEqual(consumer1, consumer2) {
// 		t.Error("test failed consumers are equal")
// 		t.Fail()
// 	}
// }

func TestMultiReceive(t *testing.T) {
	t.Log("starting router")
	router := startRouter()
	t.Cleanup(router.StopRouter)
	router.RegisterRoute("test")
	publisher := router.GetPublisher()
	for i := range 20 {
		arr := make([]byte, 4)
		binary.BigEndian.PutUint32(arr[0:4], uint32(i+1))
		publisher.Publish(arr, "test")
	}
	time.Sleep(time.Millisecond * 100)
	route, _ := router.GetRoute("test")
	if route.Size() != 20 {
		t.Errorf("test failed route has %v expected %v", route.Size(), 20)
		t.Fail()
		return
	}
	t.Log("messages sent, processing output")
	returnChan := make(chan []byte, 100)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	startCtx, start := context.WithCancel(context.Background())
	defer cancel()
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
						message.Ack()
						returnChan <- message.Content
						message.Ack()
					}
				}
			}

		}()
	}
	time.Sleep(time.Millisecond * 100)
	t.Log("workers are set")
	route, _ = router.GetRoute("test")
	t.Logf("route has %v messages", route.Size())
	start()
	<-ctx.Done()
	t.Log("time finished processing results")
	if len(returnChan) != 20 {
		t.Errorf("test failed return channel have %v expected %v", len(returnChan), 20)
		t.Fail()
		return
	}
	t.Log("checking responses")
	var num uint32
	num = 0
	for range 20 {
		content := <-returnChan
		num += binary.BigEndian.Uint32(content)
	}
	if num != 210 {
		t.Errorf("test failed sum of messages is %v expected %v", num, 210)
		t.Fail()
	}
}
