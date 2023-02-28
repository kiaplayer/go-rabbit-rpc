package rabbitmq_rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
)

type RpcClient struct {
	conn               *amqp.Connection
	channel            *amqp.Channel
	consumerTag        string
	requestsExchange   string
	responsesQueue     amqp.Queue
	requestsInProgress sync.Map
	deliveries         <-chan amqp.Delivery
	connErr            chan *amqp.Error
	logger             *log.Logger
}

type requestProcessing struct {
	responseCh chan *RpcResponse
	errorCh    chan error
}

//goland:noinspection GoUnusedExportedFunction
func NewRpcClient(url, requestsExchange string, logger *log.Logger) (*RpcClient, error) {
	c := &RpcClient{
		conn:               nil,
		channel:            nil,
		consumerTag:        "rpc-client:" + uuid.New().String(),
		requestsExchange:   requestsExchange,
		requestsInProgress: sync.Map{},
		deliveries:         make(chan amqp.Delivery),
		connErr:            make(chan *amqp.Error),
		logger:             logger,
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	c.conn, err = amqp.DialConfig(url, config)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	// Check input exchange existance
	if err = c.channel.ExchangeDeclarePassive(
		c.requestsExchange, // name of the exchange
		"direct",           // type
		true,               // durable
		false,              // delete when complete
		false,              // internal
		false,              // noWait
		nil,                // arguments
	); err != nil {
		return nil, fmt.Errorf("exchange declare: %s", err)
	}

	// Create unique queue for responses (specific for this client)
	c.responsesQueue, err = c.channel.QueueDeclare(
		"",    // empty name == unique name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue declare: %s", err)
	}

	// Subscribe for responses queue
	c.deliveries, err = c.channel.Consume(
		c.responsesQueue.Name, // name
		c.consumerTag,         // consumerTag,
		false,                 // autoAck
		false,                 // exclusive
		false,                 // noLocal
		false,                 // noWait
		nil,                   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue consume: %s", err)
	}

	c.conn.NotifyClose(c.connErr)

	// Wait for responses
	go func() {
		err := c.processResponses()
		if err != nil {
			c.logger.Printf("Error while hadling deliveries: %s\n", err.Error())
		}
	}()

	return c, nil
}

func (c *RpcClient) processResponses() error {
	for {
		select {
		case d, ok := <-c.deliveries:
			if !ok {
				return nil
			}
			requestID := d.MessageId
			if reqProcessing, found := c.requestsInProgress.Load(requestID); found {
				c.requestsInProgress.Delete(requestID)
				rpcResponse := &RpcResponse{
					Response: "",
					Error:    "",
				}
				err := json.Unmarshal(d.Body, rpcResponse)
				if err == nil {
					reqProcessing.(requestProcessing).responseCh <- rpcResponse
				} else {
					reqProcessing.(requestProcessing).errorCh <- err
				}
			}
			if err := d.Ack(false); err != nil {
				return err
			}
		case err := <-c.connErr:
			// Propagate connection problem to waiting requests
			c.requestsInProgress.Range(func(key, value any) bool {
				value.(requestProcessing).errorCh <- err
				return true
			})
			return err
		}
	}
}

func (c *RpcClient) ExecuteRpc(ctx context.Context, request *RpcRequest) (response *RpcResponse, err error) {
	if c.channel.IsClosed() {
		return nil, errors.New("connection is closed")
	}

	requestID := uuid.New().String()

	requestJson, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	responseCh := make(chan *RpcResponse)
	defer close(responseCh)
	errorCh := make(chan error)
	defer close(errorCh)

	c.requestsInProgress.Store(requestID, requestProcessing{
		responseCh: responseCh,
		errorCh:    errorCh,
	})
	defer c.requestsInProgress.Delete(requestID)

	err = c.channel.PublishWithContext(
		ctx,
		c.requestsExchange,
		"",
		true,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
			Body:            requestJson,
			MessageId:       requestID,
			ReplyTo:         c.responsesQueue.Name,
		},
	)
	if err != nil {
		return nil, err
	}

	select {
	case response := <-responseCh:
		return response, nil
	case err := <-errorCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *RpcClient) Shutdown() error {
	if !c.channel.IsClosed() {
		if err := c.channel.Cancel(c.consumerTag, true); err != nil {
			return err
		}

		if _, err := c.channel.QueueDelete(c.responsesQueue.Name, false, false, true); err != nil {
			return err
		}

		if err := c.channel.Close(); err != nil {
			return err
		}

		if err := c.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
