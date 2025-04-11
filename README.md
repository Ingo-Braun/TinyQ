# TinyQ

**A Tiny message queue implementation**

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Usage](#usage)
    1. [Creating Routers](#creating-routers)
    2. [Registering Routes](#registering-routes)
    3. [Creating Publishers and publishing Messages](#creating-publishers-and-publishing-messages)
    4. [Creating Consumers and getting Messages](#creating-consumers-and-getting-messages)
    5. [Creating an Subscriber and Callback function](#creating-an-subscriber-and-callback-function)
4. [Stopping](#stopping)

5. [Components](#components)
    - [Router](#router)
    - [Publisher](#publisher)
    - [Dedicated Publisher](#dedicated-publisher)
    - [Subscriber](#subscriber)
    - [Message](#message)

6. [FAQ](#faq)
7. [Motivation](#motivation)

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

### Creating Routers

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

### Registering Routes

```go
routeKey := "Route1"
router.RegisterRoute(routeKey, tinyQ.DefaultMaxRouteSize)
```

### Creating Publishers and publishing Messages

```go
publisher := router.GetPublisher()
message := []byte("Hello World")
messageId,ok := publisher.Publish(message,routeKey)
```

### Creating Consumers and getting Messages

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
    fmt.Println("message received")
    fmt.Println(string(message.Content))
    Ack()
}

func main() {
    router := tinyQ.Router{}
    router.InitRouter()
    defer router.StopRouter()

    routeKey := "Route1"
    router.RegisterRoute(routeKey, tinyQ.DefaultMaxRouteSize)

    publisher := router.GetPublisher()
    message := []byte("Hello World")
    fmt.Println("publishing message....")
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

    router.StopRouter()

Every **Publisher**, **Route** and **Consumers and Subscribers** wil stop working and wil be safe to delete.

## Telemetry

The router has its own built in telemetry system, to enable it just call ```router.EnableTelemetry()```.

```go
func main(){
    router := tinyQ.Router{}
    router.InitRouter()
    defer router.StopRouter()

    router.EnableTelemetry()
    telemetry := router.GetTelemetry()
    telemetry.TotalMessages
    telemetry.TotalDelivered
    telemetry.TotalLost
    telemetry.TotalReDelivered
}
```
Every time you call router.GetTelemetry() an copy of the values of the internal telemetry data is returned, it is an photo of the data.

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

## FAQ

### There is any persistence ?

No there is NO persistence. everything is in Memory.

### Is safe to call router.StopRouter() ?

Stopping any router created should never raise panic. see [`stop warning`](#warning) for better explanation.

## Motivation

I created this module because i have seen time and time again this pattern, either in an distributed environment or inside an program.
You need to pass (messages / tasks / jobs / events) to some (Handler / Worker / Tread / Process) locally or remote. you end up building a message queue system of some sort, and after building a couple of them i decided to create my own to understand how message queues works.
