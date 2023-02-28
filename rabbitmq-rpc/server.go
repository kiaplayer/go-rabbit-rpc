package rabbitmq_rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type RequestHandler interface {
	Handle(ctx context.Context, payload string) (response string, err error)
}

type RpcServer struct {
	conn                  *amqp.Connection
	channel               *amqp.Channel
	consumerTag           string
	handlers              map[string]requestHandlerParams
	defaultHandlerTimeout time.Duration
	deliveries            <-chan amqp.Delivery
	connErr               chan *amqp.Error
	logger                *log.Logger
}

type requestHandlerParams struct {
	handler RequestHandler
	timeout time.Duration
}

//goland:noinspection GoUnusedExportedFunction
func NewRpcServer(url, requestsExchange, requestsQueue string, defaultHandlerTimeout time.Duration, logger *log.Logger) (*RpcServer, error) {
	if defaultHandlerTimeout <= 0 {
		return nil, errors.New("wrong default handler timeout")
	}

	s := &RpcServer{
		consumerTag:           "rpc-server:" + uuid.New().String(),
		defaultHandlerTimeout: defaultHandlerTimeout,
		handlers:              make(map[string]requestHandlerParams),
		connErr:               make(chan *amqp.Error),
		logger:                logger,
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	s.conn, err = amqp.DialConfig(url, config)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	s.channel, err = s.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	// Create exchange for requests
	if err = s.channel.ExchangeDeclare(
		requestsExchange, // name of the exchange
		"direct",         // type
		true,             // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	); err != nil {
		return nil, fmt.Errorf("exchange declare: %s", err)
	}

	// Create queue for requests
	if _, err = s.channel.QueueDeclare(
		requestsQueue, // queue name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // noWait
		nil,           // arguments
	); err != nil {
		return nil, fmt.Errorf("queue declare: %s", err)
	}

	// Bind queue to exchange
	if err = s.channel.QueueBind(
		requestsQueue,    // queue
		"",               // bindingKey
		requestsExchange, // sourceExchange
		false,            // noWait
		nil,              // arguments
	); err != nil {
		return nil, fmt.Errorf("queue bind: %s", err)
	}

	// Subscribe for requests queue
	s.deliveries, err = s.channel.Consume(
		requestsQueue, // queue
		s.consumerTag, // consumerTag,
		false,         // autoAck
		false,         // exclusive
		false,         // noLocal
		false,         // noWait
		nil,           // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue consume: %s", err)
	}

	s.conn.NotifyClose(s.connErr)

	return s, nil
}

func (s *RpcServer) processDelivery(ctx context.Context, d amqp.Delivery) {
	messageID := d.MessageId
	replyToQueue := d.ReplyTo

	rpcRequest := &RpcRequest{}
	rpcResponse := &RpcResponse{}

	err := json.Unmarshal(d.Body, rpcRequest)
	if err == nil {
		if handlerParams, found := s.handlers[rpcRequest.Method]; found {
			handlerCtx, cancel := context.WithTimeout(ctx, handlerParams.timeout)
			defer cancel()
			rpcResponse.Response, err = handlerParams.handler.Handle(handlerCtx, rpcRequest.Payload)
			if err != nil {
				rpcResponse.Error = err.Error()
			}
		} else {
			rpcResponse.Error = "Hander for method " + rpcRequest.Method + " not found"
		}
	} else {
		rpcResponse.Error = err.Error()
	}

	if replyToQueue != "" {

		responseJson, err := json.Marshal(rpcResponse)
		if err != nil {
			s.logger.Printf("json marshal error: %s\n", err.Error())
		} else {
			err = s.channel.PublishWithContext(
				ctx,
				"",
				replyToQueue,
				true,
				false,
				amqp.Publishing{
					Headers:         amqp.Table{},
					ContentType:     "text/plain",
					ContentEncoding: "",
					DeliveryMode:    amqp.Persistent,
					Priority:        0,
					Body:            responseJson,
					MessageId:       messageID,
				},
			)
			if err != nil {
				s.logger.Printf("publish response error: %s\n", err.Error())
			}
		}
	}

	err = d.Ack(false)
	if err != nil {
		s.logger.Printf("ack error: %s\n", err.Error())
	}
}

func (s *RpcServer) AttachHandler(method string, handler RequestHandler, timeout time.Duration) {
	if timeout == 0 {
		timeout = s.defaultHandlerTimeout
	}
	s.handlers[method] = requestHandlerParams{
		handler: handler,
		timeout: timeout,
	}
}

func (s *RpcServer) DetachHandler(method string) {
	delete(s.handlers, method)
}

func (s *RpcServer) ProcessRequests(ctx context.Context) error {
	for {
		select {
		case d, ok := <-s.deliveries:
			if !ok {
				return nil
			}
			// Process every message asyncronously
			go s.processDelivery(ctx, d)
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.connErr:
			return err
		}
	}
}

func (s *RpcServer) Shutdown() error {
	if !s.channel.IsClosed() {
		if err := s.channel.Cancel(s.consumerTag, true); err != nil {
			return err
		}

		if err := s.channel.Close(); err != nil {
			return err
		}

		if err := s.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
