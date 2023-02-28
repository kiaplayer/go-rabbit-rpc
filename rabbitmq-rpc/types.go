package rabbitmq_rpc

type RpcRequest struct {
	Method  string
	Payload string
}

type RpcResponse struct {
	Error    string
	Response string
}
