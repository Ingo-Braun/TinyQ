package simple_test

import (
	"context"
	"testing"
	"time"

	"github.com/Ingo-Braun/TinyQ/messages"
	"github.com/Ingo-Braun/TinyQ/publishers/simple"
)

func TestPublish(t *testing.T) {
	t.Parallel()
	publisher := simple.SimplePublisher{}
	inputChannel := make(chan *messages.RouterMessage, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher.StartPublisher(&inputChannel, ctx)
	if publisher.IsClosed() {
		t.Error("test failed publisher is closed")
		t.FailNow()
	}
	messageId, ok := publisher.Publish([]byte("test"), "test")
	if !ok {
		t.Error("test failed publish returned false")
		t.FailNow()
	}
	// 	retrieveCtx, retrieveCancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	// 	var retrievedMessage *messages.RouterMessage
	// retrieveLoop:
	// 	for {
	// 		select {
	// 		case <-retrieveCtx.Done():
	// 			t.Error("test failed ")
	// 		}
	// 	}
	retrievedMessage := <-inputChannel
	if retrievedMessage.GetId() != messageId {
		t.Errorf("test failed retrieved message id mismatch wanted %v expected %v \n", messageId, retrievedMessage.GetId())
		t.FailNow()
	}
}

func TestPublishWithClosedPublisher(t *testing.T) {
	t.Parallel()
	publisher := simple.SimplePublisher{}
	inputChannel := make(chan *messages.RouterMessage)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher.StartPublisher(&inputChannel, ctx)
	cancel()
	time.Sleep(time.Millisecond * 100)
	if !publisher.IsClosed() {
		t.Error("test failed publisher is not closed")
		t.FailNow()
	}
	_, ok := publisher.Publish([]byte("test"), "test")
	if ok {
		t.Error("test failed publishing with closed publisher should return false")
		t.FailNow()
	}
}
