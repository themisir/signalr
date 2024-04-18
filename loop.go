package signalr

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

type loop struct {
	lastID        uint64 // Used with atomic: Must be first in struct to ensure 64bit alignment on 32bit architectures
	party         Party
	info          StructuredLogger
	dbg           StructuredLogger
	protocol      hubProtocol
	hubConn       hubConnection
	invokeContext invocationContext
	invokeClient  *invokeClient
	streamer      *streamer
	streamClient  *streamClient
	closeMessage  *closeMessage
}

func newLoop(p Party, conn Connection, protocol hubProtocol) *loop {
	protocol = reflect.New(reflect.ValueOf(protocol).Elem().Type()).Interface().(hubProtocol)
	_, dbg := p.loggers()
	protocol.setDebugLogger(dbg)
	pInfo, pDbg := p.prefixLoggers(conn.ConnectionID())
	hubConn := newHubConnection(conn, protocol, p.maximumReceiveMessageSize(), pInfo)
	streamClient := newStreamClient(protocol, p.chanReceiveTimeout(), p.streamBufferCapacity())
	streamer := &streamer{conn: hubConn}
	invokeContext := invocationContext{
		hubConn:      hubConn,
		party:        p,
		protocol:     protocol,
		streamClient: streamClient,
		streamer:     streamer,
		info:         pInfo,
		dbg:          pDbg,
	}
	return &loop{
		party:         p,
		protocol:      protocol,
		hubConn:       hubConn,
		invokeContext: invokeContext,
		invokeClient:  newInvokeClient(protocol, p.chanReceiveTimeout()),
		streamer:      streamer,
		streamClient:  streamClient,
		info:          pInfo,
		dbg:           pDbg,
	}
}

// Run runs the loop. After the startup sequence is done, this is signaled over the started channel.
// Callers should pass a channel with buffer size 1 to allow the loop to run without waiting for the caller.
func (l *loop) Run(connected chan struct{}) (err error) {
	l.party.onConnected(l.hubConn)
	connected <- struct{}{}
	close(connected)
	// Process messages
	ch := make(chan receiveResult, 1)
	wg := l.party.waitGroup()
	wg.Add(1)
	go func() {
		defer wg.Done()
		recv := l.hubConn.Receive()
	loop:
		for {
			select {
			case result, ok := <-recv:
				if !ok {
					break loop
				}
				select {
				case ch <- result:
				case <-l.hubConn.Context().Done():
					break loop
				}
			case <-l.hubConn.Context().Done():
				break loop
			}
		}
	}()
	timeoutTicker := time.NewTicker(l.party.timeout())
msgLoop:
	for {
	pingLoop:
		for {
			select {
			case evt := <-ch:
				err = evt.err
				timeoutTicker.Reset(l.party.timeout())
				if err == nil {
					switch message := evt.message.(type) {
					case invocationMessage:
						l.handleInvocationMessage(message)
					case cancelInvocationMessage:
						_ = l.dbg.Log(evt, msgRecv, msg, fmtMsg(message))
						l.streamer.Stop(message.InvocationID)
					case streamItemMessage:
						err = l.handleStreamItemMessage(message)
					case completionMessage:
						err = l.handleCompletionMessage(message)
					case closeMessage:
						_ = l.dbg.Log(evt, msgRecv, msg, fmtMsg(message))
						l.closeMessage = &message
						if message.Error != "" {
							err = errors.New(message.Error)
						}
					case hubMessage:
						// Mostly ping
						err = l.handleOtherMessage(message)
						// No default case necessary, because the protocol would return either a hubMessage or an error
					}
				} else {
					_ = l.info.Log(evt, msgRecv, "error", err, msg, fmtMsg(evt.message), react, "close connection")
				}
				break pingLoop
			case <-time.After(l.party.keepAliveInterval()):
				// Send ping only when there was no write in the keepAliveInterval before
				if time.Since(l.hubConn.LastWriteStamp()) > l.party.keepAliveInterval() {
					if err = l.hubConn.Ping(); err != nil {
						break pingLoop
					}
				}
				// A successful ping or Write shows us that the connection is alive. Reset the timeout
				timeoutTicker.Reset(l.party.timeout())
				// Don't break the pingLoop when keepAlive is over, it exists for this case
			case <-timeoutTicker.C:
				err = fmt.Errorf("timeout interval elapsed (%v)", l.party.timeout())
				break pingLoop
			case <-l.hubConn.Context().Done():
				err = fmt.Errorf("breaking loop. hubConnection canceled: %w", l.hubConn.Context().Err())
				break pingLoop
			case <-l.party.context().Done():
				err = fmt.Errorf("breaking loop. Party canceled: %w", l.party.context().Err())
				break pingLoop
			}
		}
		if err != nil || l.closeMessage != nil {
			break msgLoop
		}
	}
	l.party.onDisconnected(l.hubConn)
	if err != nil {
		_ = l.hubConn.Close(fmt.Sprintf("%v", err), l.party.allowReconnect())
	}
	_ = l.dbg.Log(evt, "message loop ended")
	l.invokeClient.cancelAllInvokes()
	l.hubConn.Abort()
	return err
}

func (l *loop) PullStream(method, id string, arguments ...interface{}) <-chan InvokeResult {
	_, errChan := l.invokeClient.newInvocation(id)
	upChan := l.streamClient.newUpstreamChannel(id)
	ch := newInvokeResultChan(l.party.context(), upChan, errChan)
	if err := l.hubConn.SendStreamInvocation(id, method, arguments); err != nil {
		// When we get an error here, the loop is closed and the errChan might be already closed
		// We create a new one to deliver our error
		ch, _ = createResultChansWithError(l.party.context(), err)
		l.streamClient.deleteUpstreamChannel(id)
		l.invokeClient.deleteInvocation(id)
	}
	return ch
}

func (l *loop) PushStreams(method, id string, arguments ...interface{}) (<-chan InvokeResult, error) {
	resultCh, errCh := l.invokeClient.newInvocation(id)
	irCh := newInvokeResultChan(l.party.context(), resultCh, errCh)
	invokeArgs := make([]interface{}, 0)
	reflectedChannels := make([]reflect.Value, 0)
	streamIds := make([]string, 0)
	// Parse arguments for channels and other kind of arguments
	for _, arg := range arguments {
		if reflect.TypeOf(arg).Kind() == reflect.Chan {
			reflectedChannels = append(reflectedChannels, reflect.ValueOf(arg))
			streamIds = append(streamIds, l.GetNewID())
		} else {
			invokeArgs = append(invokeArgs, arg)
		}
	}
	// Tell the server we are streaming now
	if err := l.hubConn.SendInvocationWithStreamIds(id, method, invokeArgs, streamIds); err != nil {
		l.invokeClient.deleteInvocation(id)
		return nil, err
	}
	// Start streaming on all channels
	for i, reflectedChannel := range reflectedChannels {
		l.streamer.Start(streamIds[i], reflectedChannel)
	}
	return irCh, nil
}

// GetNewID returns a new, connection-unique id for invocations and streams
func (l *loop) GetNewID() string {
	atomic.AddUint64(&l.lastID, 1)
	return fmt.Sprint(atomic.LoadUint64(&l.lastID))
}

func (l *loop) handleInvocationMessage(invocation invocationMessage) {
	_ = l.dbg.Log(evt, msgRecv, msg, fmtMsg(invocation))
	recv := l.party.invocationTarget(l.hubConn)
	recv.Handle(&l.invokeContext, invocation)
}

func (l *loop) handleStreamItemMessage(streamItemMessage streamItemMessage) error {
	_ = l.dbg.Log(evt, msgRecv, msg, fmtMsg(streamItemMessage))
	if err := l.streamClient.receiveStreamItem(streamItemMessage); err != nil {
		switch t := err.(type) {
		case *hubChanTimeoutError:
			_ = l.hubConn.Completion(streamItemMessage.InvocationID, nil, t.Error())
		default:
			_ = l.info.Log(evt, msgRecv, "error", err, msg, fmtMsg(streamItemMessage), react, "close connection")
			return err
		}
	}
	return nil
}

func (l *loop) handleCompletionMessage(message completionMessage) error {
	_ = l.dbg.Log(evt, msgRecv, msg, fmtMsg(message))
	var err error
	if l.streamClient.handlesInvocationID(message.InvocationID) {
		err = l.streamClient.receiveCompletionItem(message, l.invokeClient)
	} else if l.invokeClient.handlesInvocationID(message.InvocationID) {
		err = l.invokeClient.receiveCompletionItem(message)
	} else {
		err = fmt.Errorf("unknown invocationID %v", message.InvocationID)
	}
	if err != nil {
		_ = l.info.Log(evt, msgRecv, "error", err, msg, fmtMsg(message), react, "close connection")
	}
	return err
}

func (l *loop) handleOtherMessage(hubMessage hubMessage) error {
	_ = l.dbg.Log(evt, msgRecv, msg, fmtMsg(hubMessage))
	// Not Ping
	if hubMessage.Type != 6 {
		err := fmt.Errorf("invalid message type %v", hubMessage)
		_ = l.info.Log(evt, msgRecv, "error", err, msg, fmtMsg(hubMessage), react, "close connection")
		return err
	}
	return nil
}

func fmtMsg(message interface{}) string {
	switch msg := message.(type) {
	case invocationMessage:
		fmtArgs := make([]interface{}, 0)
		for _, arg := range msg.Arguments {
			if rawArg, ok := arg.(json.RawMessage); ok {
				fmtArgs = append(fmtArgs, string(rawArg))
			} else {
				fmtArgs = append(fmtArgs, arg)
			}
		}
		msg.Arguments = fmtArgs
		message = msg
	}
	return fmt.Sprintf("%#v", message)
}
