package dedicated_test

import (
	"context"
	"testing"
	"time"

	"github.com/Ingo-Braun/TinyQ/publishers/dedicated"
	"github.com/Ingo-Braun/TinyQ/route"
)

func TestPublish(t *testing.T) {
	t.Parallel()
	publisher := dedicated.DedicatedPublisher{}
	routeCtx, routeCancel := context.WithCancel(context.Background())
	defer routeCancel()
	routerCtx, routerCancel := context.WithCancel(context.Background())
	defer routerCancel()
	newRoute, routeInputChannel := route.SetupRoute(routeCtx, 10)
	publisher.StartPublisher("test", newRoute, routerCtx)
	if publisher.IsClosed() {
		t.Error("test failed publisher is closed")
		t.FailNow()
	}
	messageId, ok := publisher.Publish([]byte("test"))
	if !ok {
		t.Error("test failed publish returned false")
		t.FailNow()
	}

	retrievedMessage := <-routeInputChannel
	if retrievedMessage.GetId() != messageId {
		t.Errorf("test failed retrieved message id mismatch wanted %v expected %v \n", messageId, retrievedMessage.GetId())
		t.FailNow()
	}
}

func TestPublishWithClosedPublisher(t *testing.T) {
	t.Parallel()
	publisher := dedicated.DedicatedPublisher{}
	routeCtx, routeCancel := context.WithCancel(context.Background())
	defer routeCancel()
	routerCtx, routerCancel := context.WithCancel(context.Background())
	defer routerCancel()
	newRoute, _ := route.SetupRoute(routeCtx, 10)
	publisher.StartPublisher("test", newRoute, routerCtx)
	routeCancel()
	time.Sleep(time.Millisecond * 100)
	if publisher.IsActive() {
		t.Error("test failed publisher is active")
		t.FailNow()
	}
	_, ok := publisher.Publish([]byte("test"))
	if ok {
		t.Error("test failed publishing with closed publisher should return false")
		t.FailNow()
	}
}
