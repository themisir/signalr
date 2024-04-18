package signalr

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
)

type invocationContext struct {
	hubConn      hubConnection
	party        Party
	protocol     hubProtocol
	streamClient *streamClient
	streamer     *streamer
	info         StructuredLogger
	dbg          StructuredLogger
}

type invocationReceiver interface {
	Handle(ctx *invocationContext, invocation invocationMessage)
}

type rawInvocationReceiver struct {
	target RawReceiver
}

func (r *rawInvocationReceiver) Handle(ctx *invocationContext, invocation invocationMessage) {
	// hub method might take a long time
	go func() {
		result := func() any {
			defer recoverInvocationPanic(ctx, invocation)
			return r.target.Invoke(invocation.Target, invocation.Arguments...)
		}()
		r.returnInvocationResult(ctx, invocation, result)
	}()
}

func (r *rawInvocationReceiver) returnInvocationResult(ctx *invocationContext, invocation invocationMessage, result any) {
	// No invocation id, no completion
	if invocation.InvocationID != "" {
		// if the hub method returns a chan, it should be considered asynchronous or source for a stream
		if value := reflect.ValueOf(result); value.Kind() == reflect.Chan {
			switch invocation.Type {
			// Simple invocation
			case 1:
				go func() {
					// Recv might block, so run continue in a goroutine
					if chanResult, ok := value.Recv(); ok {
						sendResult(ctx, invocation, completion, []reflect.Value{chanResult})
					} else {
						_ = ctx.hubConn.Completion(invocation.InvocationID, nil, "hub func returned closed chan")
					}
				}()
			// StreamInvocation
			case 4:
				ctx.streamer.Start(invocation.InvocationID, value)
			}
		} else {
			switch invocation.Type {
			// Simple invocation
			case 1:
				sendResult(ctx, invocation, completion, result)
			case 4:
				// Stream invocation of method with no stream result.
				// Return a single StreamItem and an empty Completion
				sendResult(ctx, invocation, streamItem, result)
				_ = ctx.hubConn.Completion(invocation.InvocationID, nil, "")
			}
		}
	}
}

type reflectInvocationReceiver struct {
	target interface{}
}

func (r *reflectInvocationReceiver) Handle(ctx *invocationContext, invocation invocationMessage) {
	// Transient hub, dispatch invocation here
	if method, ok := getMethod(r.target, invocation.Target); !ok {
		// Unable to find the method
		_ = ctx.info.Log(evt, "getMethod", "error", "missing method", "name", invocation.Target, react, "send completion with error")
		_ = ctx.hubConn.Completion(invocation.InvocationID, nil, fmt.Sprintf("Unknown method %s", invocation.Target))
	} else if in, err := buildMethodArguments(method, invocation, ctx.streamClient, ctx.protocol); err != nil {
		// argument build failed
		_ = ctx.info.Log(evt, "buildMethodArguments", "error", err, "name", invocation.Target, react, "send completion with error")
		_ = ctx.hubConn.Completion(invocation.InvocationID, nil, err.Error())
	} else {
		// Stream invocation is only allowed when the method has only one return value
		// We allow no channel return values, because a client can receive as stream with only one item
		if invocation.Type == 4 && method.Type().NumOut() != 1 {
			_ = ctx.hubConn.Completion(invocation.InvocationID, nil,
				fmt.Sprintf("Stream invocation of method %s which has not return value kind channel", invocation.Target))
		} else {
			// hub method might take a long time
			go func() {
				result := func() []reflect.Value {
					defer recoverInvocationPanic(ctx, invocation)
					return method.Call(in)
				}()
				r.returnInvocationResult(ctx, invocation, result)
			}()
		}
	}
}

func recoverInvocationPanic(ctx *invocationContext, invocation invocationMessage) {
	if err := recover(); err != nil {
		_ = ctx.info.Log(evt, "panic in target method", "error", err, "name", invocation.Target, react, "send completion with error")
		stack := string(debug.Stack())
		_ = ctx.dbg.Log(evt, "panic in target method", "error", err, "name", invocation.Target, react, "send completion with error", "stack", stack)
		if invocation.InvocationID != "" {
			if !ctx.party.enableDetailedErrors() {
				stack = ""
			}
			_ = ctx.hubConn.Completion(invocation.InvocationID, nil, fmt.Sprintf("%v\n%v", err, stack))
		}
	}
}

func (r *reflectInvocationReceiver) returnInvocationResult(ctx *invocationContext, invocation invocationMessage, result []reflect.Value) {
	// No invocation id, no completion
	if invocation.InvocationID != "" {
		// if the hub method returns a chan, it should be considered asynchronous or source for a stream
		if len(result) == 1 && result[0].Kind() == reflect.Chan {
			switch invocation.Type {
			// Simple invocation
			case 1:
				go func() {
					// Recv might block, so run continue in a goroutine
					if chanResult, ok := result[0].Recv(); ok {
						sendResult(ctx, invocation, completion, []reflect.Value{chanResult})
					} else {

						_ = ctx.hubConn.Completion(invocation.InvocationID, nil, "hub func returned closed chan")
					}
				}()
			// StreamInvocation
			case 4:
				ctx.streamer.Start(invocation.InvocationID, result[0])
			}
		} else {
			switch invocation.Type {
			// Simple invocation
			case 1:
				sendResult(ctx, invocation, completion, result)
			case 4:
				// Stream invocation of method with no stream result.
				// Return a single StreamItem and an empty Completion
				sendResult(ctx, invocation, streamItem, result)
				_ = ctx.hubConn.Completion(invocation.InvocationID, nil, "")
			}
		}
	}
}

func sendResult(ctx *invocationContext, invocation invocationMessage, connFunc connFunc, result any) {
	switch v := result.(type) {
	case []reflect.Value:
		switch len(v) {
		case 0:
			_ = ctx.hubConn.Completion(invocation.InvocationID, nil, "")
		case 1:
			connFunc(ctx, invocation, v[0].Interface())
		default:
			values := make([]interface{}, len(v))
			for i, rv := range v {
				values[i] = rv.Interface()
			}
			connFunc(ctx, invocation, values)
		}

	case nil:
		_ = ctx.hubConn.Completion(invocation.InvocationID, nil, "")

	default:
		connFunc(ctx, invocation, result)
	}

}

type connFunc func(ctx *invocationContext, invocation invocationMessage, value interface{})

func streamItem(ctx *invocationContext, invocation invocationMessage, value interface{}) {
	_ = ctx.hubConn.StreamItem(invocation.InvocationID, value)
}

func completion(ctx *invocationContext, invocation invocationMessage, value interface{}) {
	_ = ctx.hubConn.Completion(invocation.InvocationID, value, "")
}

func getMethod(target interface{}, name string) (reflect.Value, bool) {
	hubType := reflect.TypeOf(target)
	if hubType != nil {
		hubValue := reflect.ValueOf(target)
		name = strings.ToLower(name)
		for i := 0; i < hubType.NumMethod(); i++ {
			// Search in public methods
			if m := hubType.Method(i); strings.ToLower(m.Name) == name {
				return hubValue.Method(i), true
			}
		}
	}
	return reflect.Value{}, false
}

func buildMethodArguments(method reflect.Value, invocation invocationMessage,
	streamClient *streamClient, protocol hubProtocol) (arguments []reflect.Value, err error) {
	if len(invocation.StreamIds)+len(invocation.Arguments) != method.Type().NumIn() {
		return nil, fmt.Errorf("parameter mismatch calling method %v", invocation.Target)
	}
	arguments = make([]reflect.Value, method.Type().NumIn())
	chanCount := 0
	for i := 0; i < method.Type().NumIn(); i++ {
		t := method.Type().In(i)
		// Is it a channel for client streaming?
		if arg, clientStreaming, err := streamClient.buildChannelArgument(invocation, t, chanCount); err != nil {
			// it is, but channel count in invocation and method mismatch
			return nil, err
		} else if clientStreaming {
			// it is
			chanCount++
			arguments[i] = arg
		} else {
			// it is not, so do the normal thing
			arg := reflect.New(t)
			if err := protocol.UnmarshalArgument(invocation.Arguments[i-chanCount], arg.Interface()); err != nil {
				return arguments, err
			}
			arguments[i] = arg.Elem()
		}
	}
	if len(invocation.StreamIds) != chanCount {
		return arguments, fmt.Errorf("to many StreamIds for channel parameters of method %v", invocation.Target)
	}
	return arguments, nil
}
