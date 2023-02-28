package main

import (
	"context"
	"fmt"
	rabbitmqrpc "github.com/kiaplayer/go-rabbit-rpc/rabbitmq-rpc"
	"log"
	"os"
	"time"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	client, err := rabbitmqrpc.NewRpcClient("amqp://guest:guest@127.0.0.1:5672/", "rpc", logger)
	if err != nil {
		logger.Panic(err.Error())
	}
	defer func() {
		err := client.Shutdown()
		if err != nil {
			logger.Printf("Client shutdown error: %s\n", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	response, err := client.ExecuteRpc(ctx, &rabbitmqrpc.RpcRequest{
		Method:  "v1/echo",
		Payload: "payload",
	})
	elapsed := time.Since(start)
	fmt.Printf("Execution time: %s\nServer response: %+v\n", elapsed.String(), response)
}
