package main

import (
	"context"
	rabbitmqrpc "github.com/kiaplayer/go-rabbit-rpc/rabbitmq-rpc"
	"log"
	"os"
	"time"
)

type GetEchoHandler struct{}

func (h *GetEchoHandler) Handle(_ context.Context, payload string) (response string, err error) {
	return payload, nil
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	server, err := rabbitmqrpc.NewRpcServer(
		"amqp://guest:guest@127.0.0.1:5672/",
		"rpc", "requests",
		10*time.Millisecond,
		logger,
	)
	if err != nil {
		logger.Panic(err.Error())
	}
	defer func(server *rabbitmqrpc.RpcServer) {
		err := server.Shutdown()
		if err != nil {
			logger.Printf("Server shutdown error: %s\n", err.Error())
		}
	}(server)

	server.AttachHandler("v1/echo", &GetEchoHandler{}, 0)

	logger.Println("Server started...")

	if err = server.ProcessRequests(context.TODO()); err != nil {
		logger.Printf("Process requests error: %s\n", err.Error())
	}
}
