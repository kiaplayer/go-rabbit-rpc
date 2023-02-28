# RPC pattern for Golang (using RabbitMQ)

## Install package
```
$ go get github.com/kiaplayer/go-rabbit-rpc
```

## Usage example

See [example](example) folder for example source code.

You can test it with RabbitMQ in Docker container:
```
$ docker run --rm --name rabbitmq \
  -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest  \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3.10-management

$ cd example
$ go mod download

$ go run server.go
2023/02/28 15:39:31 Server started...

$ go run client.go
Execution time: 3.201844ms
Server response: &{Error: Response:payload}
```
