package da

import (
    "fmt"
    "log"
    "gopkg.in/zeromq/goczmq.v4"
)

// ZmqSubscriber handles common ZMQ subscription logic
type ZmqSubscriber struct {
    Channeler *goczmq.Channeler
    Topic     string
    Endpoint  string
}

// NewZmqSubscriber creates and initializes a new ZMQ subscriber
func NewZmqSubscriber(endpoint string, topic string) (*ZmqSubscriber, error) {
    channeler := goczmq.NewSubChanneler(endpoint, topic)
    if channeler == nil {
        return nil, fmt.Errorf("error creating channeler")
    }
    
    return &ZmqSubscriber{
        Channeler: channeler,
        Topic:     topic,
        Endpoint:  endpoint,
    }, nil
}

// ValidateMessage validates the ZMQ message format
func (z *ZmqSubscriber) ValidateMessage(msg [][]byte) (bool, error) {
    if len(msg) != 3 {
        return false, fmt.Errorf("received message with unexpected number of parts")
    }
    topic := string(msg[0])
    if topic != z.Topic {
        return false, fmt.Errorf("unexpected topic: %s", topic)
    }
    return true, nil
}

// Listen starts listening for messages and processes them using the provided handler
func (z *ZmqSubscriber) Listen(handler func([][]byte) error) {
    fmt.Printf("Listening for messages on topic %s from endpoint %s...\n", z.Topic, z.Endpoint)
    for {
        select {
        case msg, ok := <-z.Channeler.RecvChan:
            if !ok {
                log.Println("Failed to receive message")
                continue
            }
            
            valid, err := z.ValidateMessage(msg)
            if !valid {
                log.Printf("Message validation failed: %v", err)
                continue
            }

            if err := handler(msg); err != nil {
                log.Printf("Error processing message: %v", err)
                continue
            }
        }
    }
}

// Close cleans up the ZMQ channeler
func (z *ZmqSubscriber) Close() {
    if z.Channeler != nil {
        z.Channeler.Destroy()
    }
}
