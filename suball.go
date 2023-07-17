package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"golang.org/x/sync/errgroup"
)

type ProxyPubSub struct {
	opt *Options

	// 创建到某一实例的非池化连接
	newConn   func(ctx context.Context, addr string, channels []string) (*pool.Conn, error)
	closeConn func(cn *pool.Conn) error

	mu  sync.Mutex
	cns map[string]*pool.Conn

	channels map[string]struct{}
	patterns map[string]struct{}

	closed bool
	exit   chan struct{}

	cmd *Cmd

	notifyCh    chan interface{}
	inSubscribe map[*pool.Conn]struct{}

	chOnce sync.Once
	msgCh  *channel
	allCh  *channel
}

func (c *ProxyPubSub) init() {
	c.exit = make(chan struct{})
	c.cns = make(map[string]*pool.Conn)
	c.notifyCh = make(chan interface{})
	c.inSubscribe = make(map[*pool.Conn]struct{})
}

func (c *ProxyPubSub) String() string {
	channels := mapKeys(c.channels)
	channels = append(channels, mapKeys(c.patterns)...)
	return fmt.Sprintf("ProxyPubSub(%s)", strings.Join(channels, ", "))
}

func (c *ProxyPubSub) connWithLock(ctx context.Context) (map[string]*pool.Conn, error) {
	c.mu.Lock()
	cn, err := c.conn(ctx, nil)
	c.mu.Unlock()
	return cn, err
}

// 对每个proxy实例返回一个连接
// 会对新增的proxy实例建连 和 断开下线的proxy实例
func (c *ProxyPubSub) conn(ctx context.Context, newChannels []string) (map[string]*pool.Conn, error) {
	if c.closed {
		return nil, pool.ErrClosed
	}

	var currentProxy []string
	if c.opt.ObserverDisable {
		currentProxy = c.opt.InitServers
	} else {
		currentProxy = ob.getCurrentProxyList()
	}

	// 移除下线proxy的连接
	for addr, cn := range c.cns {
		if exist := contains(currentProxy, addr); !exist {
			delete(c.inSubscribe, cn)
			_ = c.closeConn(cn)
		}
	}

	// 新建到该proxy的连接
	for _, addr := range currentProxy {
		if _, exist := c.cns[addr]; !exist {
			cn, err := c.newConn(ctx, addr, newChannels)
			if err != nil {
				return nil, err
			}

			if err := c.resubscribe(ctx, cn); err != nil {
				_ = c.closeConn(cn)
				return nil, err
			}
			c.cns[addr] = cn
		}
	}

	if len(c.cns) <= 0 {
		return nil, errors.New("not proxy for subscribe")
	}

	return c.cns, nil
}

func (c *ProxyPubSub) writeCmd(ctx context.Context, cn *pool.Conn, cmd Cmder) error {
	return cn.WithWriter(context.Background(), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, cmd)
	})
}

func (c *ProxyPubSub) resubscribe(ctx context.Context, cn *pool.Conn) error {
	var firstErr error

	if len(c.channels) > 0 {
		firstErr = c._subscribe(ctx, cn, "subscribe", mapKeys(c.channels))
	}

	if len(c.patterns) > 0 {
		err := c._subscribe(ctx, cn, "psubscribe", mapKeys(c.patterns))
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (c *ProxyPubSub) _subscribe(
	ctx context.Context, cn *pool.Conn, redisCmd string, channels []string,
) error {
	args := make([]interface{}, 0, 1+len(channels))
	args = append(args, redisCmd)
	for _, channel := range channels {
		args = append(args, channel)
	}
	cmd := NewSliceCmd(ctx, args...)
	return c.writeCmd(ctx, cn, cmd)
}

func (c *ProxyPubSub) closeTheCn(reason error) error {
	if len(c.cns) == 0 {
		return nil
	}
	if !c.closed {
		internal.Logger.Printf(c.getContext(), "redis: discarding bad PubSub connection: %s", reason)
	}

	for _, cn := range c.cns {
		if err := c.closeConn(cn); err != nil {
			return err
		}
	}

	for k := range c.cns {
		delete(c.cns, k)
	}
	return nil
}

func (c *ProxyPubSub) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return pool.ErrClosed
	}
	c.closed = true
	close(c.exit)

	return c.closeTheCn(pool.ErrClosed)
}

// Subscribe the client to the specified channels. It returns
// empty subscription if there are no channels.
func (c *ProxyPubSub) Subscribe(ctx context.Context, channels ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.subscribe(ctx, "subscribe", channels...)
	if c.channels == nil {
		c.channels = make(map[string]struct{})
	}
	for _, s := range channels {
		c.channels[s] = struct{}{}
	}
	return err
}

// PSubscribe the client to the given patterns. It returns
// empty subscription if there are no patterns.
func (c *ProxyPubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.subscribe(ctx, "psubscribe", patterns...)
	if c.patterns == nil {
		c.patterns = make(map[string]struct{})
	}
	for _, s := range patterns {
		c.patterns[s] = struct{}{}
	}
	return err
}

// Unsubscribe the client from the given channels, or from all of
// them if none is given.
func (c *ProxyPubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(channels) > 0 {
		for _, channel := range channels {
			delete(c.channels, channel)
		}
	} else {
		// Unsubscribe from all channels.
		for channel := range c.channels {
			delete(c.channels, channel)
		}
	}

	err := c.subscribe(ctx, "unsubscribe", channels...)
	return err
}

// PUnsubscribe the client from the given patterns, or from all of
// them if none is given.
func (c *ProxyPubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(patterns) > 0 {
		for _, pattern := range patterns {
			delete(c.patterns, pattern)
		}
	} else {
		// Unsubscribe from all patterns.
		for pattern := range c.patterns {
			delete(c.patterns, pattern)
		}
	}

	err := c.subscribe(ctx, "punsubscribe", patterns...)
	return err
}

func (c *ProxyPubSub) subscribe(ctx context.Context, redisCmd string, channels ...string) error {
	cns, err := c.conn(ctx, channels)
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	for _, cn := range cns {
		localCn := cn
		eg.Go(func() error {
			fmt.Printf("conn:%s sub:%v\n", localCn.RemoteAddr(), channels)
			return c._subscribe(ctx, localCn, redisCmd, channels)
		})
	}

	if err = eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Ping will ping all proxy instance
func (c *ProxyPubSub) Ping(ctx context.Context, payload ...string) error {
	args := []interface{}{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	}
	cmd := NewCmd(ctx, args...)

	c.mu.Lock()
	defer c.mu.Unlock()

	cns, err := c.conn(ctx, nil)
	if err != nil {
		return err
	}

	for _, cn := range cns {
		if err = c.writeCmd(ctx, cn, cmd); err != nil {
			return err
		}
	}

	return err
}

func (c *ProxyPubSub) newMessage(reply interface{}) (interface{}, error) {
	switch reply := reply.(type) {
	case string:
		return &Pong{
			Payload: reply,
		}, nil
	case []interface{}:
		switch kind := reply[0].(string); kind {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe":
			// Can be nil in case of "unsubscribe".
			channel, _ := reply[1].(string)
			return &Subscription{
				Kind:    kind,
				Channel: channel,
				Count:   int(reply[2].(int64)),
			}, nil
		case "message", "smessage":
			switch payload := reply[2].(type) {
			case string:
				return &Message{
					Channel: reply[1].(string),
					Payload: payload,
				}, nil
			case []interface{}:
				ss := make([]string, len(payload))
				for i, s := range payload {
					ss[i] = s.(string)
				}
				return &Message{
					Channel:      reply[1].(string),
					PayloadSlice: ss,
				}, nil
			default:
				return nil, fmt.Errorf("redis: unsupported pubsub message payload: %T", payload)
			}
		case "pmessage":
			return &Message{
				Pattern: reply[1].(string),
				Channel: reply[2].(string),
				Payload: reply[3].(string),
			}, nil
		case "pong":
			return &Pong{
				Payload: reply[1].(string),
			}, nil
		default:
			return nil, fmt.Errorf("redis: unsupported pubsub message: %q", kind)
		}
	default:
		return nil, fmt.Errorf("redis: unsupported pubsub message: %#v", reply)
	}
}

// ReceiveTimeout acts like Receive but returns an error if message
// is not received in time. This is low-level API and in most cases
// Channel should be used instead.
func (c *ProxyPubSub) ReceiveTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	if c.cmd == nil {
		c.cmd = NewCmd(ctx)
	}

	conns, err := c.connWithLock(ctx)
	if err != nil {
		return nil, err
	}

	for addr, cn := range conns {
		if _, exist := c.inSubscribe[cn]; !exist {
			c.inSubscribe[cn] = struct{}{}
			localAddr, localCn := addr, cn
			go func() {
				internal.Logger.Printf(ctx, "create goroutine for subscribe proxy:%s\n", localAddr)

				t := timeout / 2
				if t <= 0 {
					t = 1 * time.Second
				}

				for {
					select {
					case _, ok := <-c.exit:
						if !ok {
							delete(c.inSubscribe, localCn)
							internal.Logger.Printf(ctx, "exit subscribe goroutine of proxy:%s\n", localAddr)
							return
						}
					default:
						// 一定要设置timeout, 让goroutine有机会退出
						if err = localCn.WithReader(context.Background(), t, func(rd *proto.Reader) error {
							v, err := rd.ReadReply()
							if v != nil {
								c.cmd.val = v
								c.notifyCh <- struct{}{}
							}
							return err
						}); err != nil {
							// 忽略内部的超时
							if e, ok := err.(*net.OpError); !(ok && e.Timeout()) {
								c.notifyCh <- err
							}
						}
					}
				}
			}()
		}
	}

	// 超时不会断开连接
	// 模拟超时逻辑
	var received interface{}
	if timeout > 0 {
		for {
			select {
			case received = <-c.notifyCh:
				goto EXIT
			case <-time.After(timeout):
				received = errors.New("subscribe timeout")
				goto EXIT
			}
		}
	} else {
		received = <-c.notifyCh
	}

EXIT:
	if _, ok := received.(error); ok {
		return nil, err
	}
	return c.newMessage(c.cmd.Val())
}

// Receive returns a message as a Subscription, Message, Pong or error.
// See PubSub example for details. This is low-level API and in most cases
// Channel should be used instead.
func (c *ProxyPubSub) Receive(ctx context.Context) (interface{}, error) {
	return c.ReceiveTimeout(ctx, 0)
}

// ReceiveMessage returns a Message or error ignoring Subscription and Pong
// messages. This is low-level API and in most cases Channel should be used
// instead.
func (c *ProxyPubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	for {
		msg, err := c.Receive(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *Subscription:
			// Ignore.
		case *Pong:
			// Ignore.
		case *Message:
			return msg, nil
		default:
			err := fmt.Errorf("redis: unknown message: %T", msg)
			return nil, err
		}
	}
}

func (c *ProxyPubSub) getContext() context.Context {
	if c.cmd != nil {
		return c.cmd.ctx
	}
	return context.Background()
}

//------------------------------------------------------------------------------

// Channel returns a Go channel for concurrently receiving messages.
// The channel is closed together with the ProxyPubSub. If the Go channel
// is blocked full for 30 seconds the message is dropped.
// Receive* APIs can not be used after channel is created.
//
// go-redis periodically sends ping messages to test connection health
// and re-subscribes if ping can not not received for 30 seconds.
func (c *ProxyPubSub) Channel(opts ...ChannelOption) <-chan *Message {
	c.chOnce.Do(func() {
		c.msgCh = newProxyChannel(c, opts...)
		c.msgCh.initMsgChanProxy()
	})
	if c.msgCh == nil {
		err := fmt.Errorf("redis: Channel can't be called after ChannelWithSubscriptions")
		panic(err)
	}
	return c.msgCh.msgCh
}

// ChannelSize is like Channel, but creates a Go channel
// with specified buffer size.
//
// Deprecated: use Channel(WithChannelSize(size)), remove in v9.
func (c *ProxyPubSub) ChannelSize(size int) <-chan *Message {
	return c.Channel(WithChannelSize(size))
}

// ChannelWithSubscriptions is like Channel, but message type can be either
// *Subscription or *Message. Subscription messages can be used to detect
// reconnections.
//
// ChannelWithSubscriptions can not be used together with Channel or ChannelSize.
func (c *ProxyPubSub) ChannelWithSubscriptions(opts ...ChannelOption) <-chan interface{} {
	c.chOnce.Do(func() {
		c.allCh = newProxyChannel(c, opts...)
		c.allCh.initAllChanProxy()
	})
	if c.allCh == nil {
		err := fmt.Errorf("redis: ChannelWithSubscriptions can't be called after Channel")
		panic(err)
	}
	return c.allCh.allCh
}

func newProxyChannel(proxyPubSub *ProxyPubSub, opts ...ChannelOption) *channel {
	c := &channel{
		proxyPubSub: proxyPubSub,

		chanSize:        100,
		chanSendTimeout: time.Minute,
		checkInterval:   3 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.checkInterval > 0 {
		c.initHealthCheckProxy()
	}
	return c
}

// initMsgChan must be in sync with initAllChan.
func (c *channel) initMsgChanProxy() {
	ctx := context.TODO()
	c.msgCh = make(chan *Message, c.chanSize)

	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()

		var errCount int
		for {
			msg, err := c.proxyPubSub.Receive(ctx)
			if err != nil {
				if err == pool.ErrClosed {
					close(c.msgCh)
					return
				}
				if errCount > 0 {
					time.Sleep(100 * time.Millisecond)
				}
				errCount++
				continue
			}

			errCount = 0

			// Any message is as good as a ping.
			select {
			case c.ping <- struct{}{}:
			default:
			}

			switch msg := msg.(type) {
			case *Subscription:
				// Ignore.
			case *Pong:
				// Ignore.
			case *Message:
				timer.Reset(c.chanSendTimeout)
				select {
				case c.msgCh <- msg:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					internal.Logger.Printf(
						ctx, "redis: %s channel is full for %s (message is dropped)",
						c, c.chanSendTimeout)
				}
			default:
				internal.Logger.Printf(ctx, "redis: unknown message type: %T", msg)
			}
		}
	}()
}

func (c *channel) initAllChanProxy() {
	ctx := context.TODO()
	c.allCh = make(chan interface{}, c.chanSize)

	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()

		var errCount int
		for {
			msg, err := c.proxyPubSub.Receive(ctx)
			if err != nil {
				if err == pool.ErrClosed {
					close(c.allCh)
					return
				}
				if errCount > 0 {
					time.Sleep(100 * time.Millisecond)
				}
				errCount++
				continue
			}

			errCount = 0

			// Any message is as good as a ping.
			select {
			case c.ping <- struct{}{}:
			default:
			}

			switch msg := msg.(type) {
			case *Pong:
				// Ignore.
			case *Subscription, *Message:
				timer.Reset(c.chanSendTimeout)
				select {
				case c.allCh <- msg:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					internal.Logger.Printf(
						ctx, "redis: %s channel is full for %s (message is dropped)",
						c, c.chanSendTimeout)
				}
			default:
				internal.Logger.Printf(ctx, "redis: unknown message type: %T", msg)
			}
		}
	}()
}

func (c *channel) initHealthCheckProxy() {
	ctx := context.TODO()
	c.ping = make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()

		for {
			timer.Reset(c.checkInterval)
			select {
			case <-c.ping:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				if pingErr := c.proxyPubSub.Ping(ctx); pingErr != nil {
					c.proxyPubSub.mu.Lock()
					c.proxyPubSub.reconnect(ctx, pingErr)
					c.proxyPubSub.mu.Unlock()
				}
			case <-c.proxyPubSub.exit:
				return
			}
		}
	}()
}

func (c *ProxyPubSub) reconnect(ctx context.Context, reason error) {
	_ = c.closeTheCn(reason)
	_, _ = c.conn(ctx, nil)
}
