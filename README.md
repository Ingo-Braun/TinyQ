# TinyQ

**A Tiny message queue implementation**

## Overview

Greetings this library implements the basics of an message queue system.

TinyQ implements:

- Publishers
- Subscribers
- Consumers
- Message Confirmation (ACK)

## Installation

    go get github.com/Ingo-Braun/TinyQ

## Usage

### Creating the Router

```go

package main

import (
    "github.com/Ingo-Braun/TinyQ"
)

func main(){
    router := tinyQ.Router{}
    router.InitRouter()
    defer router.StopRouter()
}
```

### Registering the Route

```go
routeKey := "Route1"
router.RegisterRoute(routeKey)
```

### Creating an Publisher and publishing an message

```go
publisher := router.GetPublisher()
message := []byte("Hello World")
messageId,ok := publisher.Publish(message,routeKey)
```

### Creating an Consumer and getting an message

```go
consumer := router.GetConsumer(routeKey)
message,ok := consumer.GetMessage()
consumer.Ack(message)
```

### Creating an Subscriber and Callback function

```go
package main

import (
    "context"
    "fmt"

    tinyQ "github.com/Ingo-Braun/TinyQ"
    Messages "github.com/Ingo-Braun/TinyQ/messages"
)

func callback(message *Messages.RouterMessage, Ack context.CancelFunc) {
    fmt.Println(string(message.Content))
    Ack()
}

func main() {
    router := tinyQ.Router{}
    router.InitRouter()
    defer router.StopRouter()

    routeKey := "Route1"
    router.RegisterRoute(routeKey)

    publisher := router.GetPublisher()
    message := []byte("Hello World")
    publisher.Publish(message, routeKey)

    subscriber, _ := router.GetSubscriber(routeKey, callback)
    subscriber.Join()
}

```

## Stopping

Stopping is straightforward

To stop the router call **StopRouter**

To stop an Route either call on router StopRoute passing the route key or on the Route it self call **CloseRoute**

To stop an subscriber call **Close**

All publishers, dedicated publishers and consumers wil close when either the **Router** or the **Route** is **Closed**

### Warning

When closing either the **Router** or **Route** is **Closed** it cascades closing **Everything** attached to it

The Close cascade goes from: **Router** -> **Publisher** -> **Route** -> **Dedicated Publishers** -> **Consumers and Subscribers**

If you call

    Router.StopRouter()

Every **Publisher**, **Route** and **Consumers and Subscribers** wil stop working and wil be safe to delete

## Components

### Router

    The Router is the heart of TinyQ. he is responsible for:

        - Routing messages from it`s own input queue to destination Routes.
        - Handling with destination Routes (Registration and Deletion)
        - Attaching to it self Publishers, Subscribers and Consumers

    On calling the StopRouter function all messages routing will stop, all attached Publishers, Subscribers and Consumers will stop

### Publisher

    The publisher is responsible for publishing messages to an Router using an RouteKey.
    When Publish is called it returns the publisher message id (string) and an ok flag (bool).
    If the Router is stopped the id will always be an empty string and ok will be false.

### Dedicated Publisher

    The Dedicated Publisher is similar to the Publisher.
    Instead of publishing messages to the Router input it sends direct to the Route itself
    Dedicated Publishers allow a lower latency and more direct approach with the drawback of being Dedicated to one Route
    If the Router is stopped or the bonded Route closes the message id will always be an empty string and ok will be false.

### Consumer

    The consumer is the basic form of getting an message from it`s attached Route.
    When GetMessage is called the consumer wil try to retrieve an message for 200 ms.
    If an message is retrieved than will return an Message (RouterMessage) and an ok flag (bool). and start the delivery timer.
    If it fails to retrieve an message will return nil,false.
    To confirm an message use it`s Ack  method passing the message.
    When confirming an message an flag (bool) will be returned indicating success on acknowledge

### Subscriber

    The Subscriber is an more elaborate way of getting messages.
    The Subscriber works on an Callback basis. when an message is retrieved it invokes the callback
    The callback function needs to have both message *Messages.RouterMessage and Ack context.CancelFunc as parameters

### Message

    The Message is the basic form of Routing message and it contains all the information necessary to pass information from the input Route to the destination Route.
    The message contents need to be an array of byte ([]byte).
    The message Route is the Routing Key used to route the messages
    Warning DO NOT use the Ack function

