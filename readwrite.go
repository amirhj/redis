package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v7/internal"
	"github.com/go-redis/redis/v7/internal/pool"
	"github.com/go-redis/redis/v7/internal/proto"
)

func (c *RWClient) Process(cmd Cmder) error {
	return c.ProcessContext(c.ctx, cmd)
}

func (c *RWClient) ProcessContext(ctx context.Context, cmd Cmder) error {
	return c.hooks.process(ctx, cmd, c.rwClient.process)
}

func (c *rwClient) process(ctx context.Context, cmd Cmder) error {
	err := c._process(ctx, cmd)
	if err != nil {
		cmd.SetErr(err)
		return err
	}
	return nil
}

func (c *rwClient) _process(ctx context.Context, cmd Cmder) error {
	var err error
	info := c.cmdInfo(cmd.Name())
	if info.ReadOnly {
		err = c.proccesCmd(ctx, c.opt.Read, cmd, c.readConnPool)
	} else {
		err = c.proccesCmd(ctx, c.opt.Write, cmd, c.writeConnPool)
	}

	return err
}

func (c *rwClient) proccesCmd(ctx context.Context, opt *Options, cmd Cmder, connPool pool.Pooler) error {
	var lastErr error
	for attempt := 0; attempt <= opt.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt, opt)); err != nil {
				return err
			}
		}

		retryTimeout := true
		lastErr = c.withConn(ctx, opt, connPool, func(ctx context.Context, cn *pool.Conn) error {
			err := cn.WithWriter(ctx, opt.WriteTimeout, func(wr *proto.Writer) error {
				return writeCmd(wr, cmd)
			})
			if err != nil {
				return err
			}

			err = cn.WithReader(ctx, c.cmdTimeout(cmd, opt), cmd.readReply)
			if err != nil {
				retryTimeout = cmd.readTimeout() == nil
				return err
			}

			return nil
		})
		if lastErr == nil || !isRetryableError(lastErr, retryTimeout) {
			return lastErr
		}
	}
	return lastErr
}

func (c *rwClient) cmdInfo(name string) *CommandInfo {
	cmdsInfo, err := c.cmdsInfoCache.Get()
	if err != nil {
		return nil
	}

	info := cmdsInfo[name]
	if info == nil {
		internal.Logger.Printf("info for cmd=%s not found", name)
	}
	return info
}

func (c *RWClient) cmdsInfo() (map[string]*CommandInfo, error) {
	info, err := c.Command().Result()
	if err == nil {
		return info, nil
	}

	return nil, err
}

func (c *rwClient) releaseConn(cn *pool.Conn, opt *Options, connPool pool.Pooler, err error) {
	if opt.Limiter != nil {
		opt.Limiter.ReportResult(err)
	}

	if isBadConn(err, false) {
		connPool.Remove(cn, err)
	} else {
		connPool.Put(cn)
	}
}

func (c *rwClient) withConn(
	ctx context.Context, opt *Options, connPool pool.Pooler, fn func(context.Context, *pool.Conn) error,
) error {
	cn, err := c.getConn(ctx, opt, connPool)
	if err != nil {
		return err
	}
	defer func() {
		c.releaseConn(cn, opt, connPool, err)
	}()

	err = fn(ctx, cn)
	return err
}

func (c *rwClient) getConn(ctx context.Context, opt *Options, connPool pool.Pooler) (*pool.Conn, error) {
	if opt.Limiter != nil {
		err := opt.Limiter.Allow()
		if err != nil {
			return nil, err
		}
	}

	cn, err := c._getConn(ctx, opt, connPool)
	if err != nil {
		if opt.Limiter != nil {
			opt.Limiter.ReportResult(err)
		}
		return nil, err
	}
	return cn, nil
}

func (c *rwClient) _getConn(ctx context.Context, opt *Options, connPool pool.Pooler) (*pool.Conn, error) {
	cn, err := connPool.Get(ctx)
	if err != nil {
		return nil, err
	}

	err = c.initConn(ctx, cn, opt)
	if err != nil {
		connPool.Remove(cn, err)
		if err := internal.Unwrap(err); err != nil {
			return nil, err
		}
		return nil, err
	}

	return cn, nil
}

func (c *rwClient) initConn(ctx context.Context, cn *pool.Conn, opt *Options) error {
	if cn.Inited {
		return nil
	}
	cn.Inited = true

	if opt.Password == "" &&
		opt.DB == 0 &&
		!opt.readOnly &&
		opt.OnConnect == nil {
		return nil
	}

	connPool := pool.NewSingleConnPool(nil)
	connPool.SetConn(cn)
	conn := newConn(ctx, opt, connPool)

	_, err := conn.Pipelined(func(pipe Pipeliner) error {
		if opt.Password != "" {
			pipe.Auth(opt.Password)
		}

		if opt.DB > 0 {
			pipe.Select(opt.DB)
		}

		if opt.readOnly {
			pipe.ReadOnly()
		}

		return nil
	})
	if err != nil {
		return err
	}

	if opt.OnConnect != nil {
		return opt.OnConnect(conn)
	}
	return nil
}

func (c *rwClient) retryBackoff(attempt int, opt *Options) time.Duration {
	return internal.RetryBackoff(attempt, opt.MinRetryBackoff, opt.MaxRetryBackoff)
}

func (c *rwClient) cmdTimeout(cmd Cmder, opt *Options) time.Duration {
	if timeout := cmd.readTimeout(); timeout != nil {
		t := *timeout
		if t == 0 {
			return 0
		}
		return t + 10*time.Second
	}
	return opt.ReadTimeout
}

// Close closes the client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *rwClient) Close() error {
	var firstErr error
	if c.onClose != nil {
		if err := c.onClose(); err != nil {
			firstErr = err
		}
	}
	if err := c.readConnPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := c.writeConnPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

type rwClient struct {
	opt           *RWOptions
	readConnPool  pool.Pooler
	writeConnPool pool.Pooler
	cmdsInfoCache *cmdsInfoCache

	onClose func() error // hook called when client is closed
}

//------------------------------------------------------------------------------

type RWOptions struct {
	Read  *Options
	Write *Options
}

// RWClient is a Redis Cluster client representing a pool of zero
// or more underlying connections. It's safe for concurrent use by
// multiple goroutines.
type RWClient struct {
	*rwClient
	cmdable
	hooks
	ctx context.Context
}

// NewRWClient returns a client to the Redis Servers specified by Options.
func NewRWClient(opt *RWOptions) *RWClient {
	opt.Read.init()
	opt.Write.init()

	c := RWClient{
		rwClient: newRWClient(opt),
		ctx:      context.Background(),
	}
	c.cmdsInfoCache = newCmdsInfoCache(c.cmdsInfo)
	c.cmdable = c.Process

	return &c
}

func newRWClient(opt *RWOptions) *rwClient {
	return &rwClient{
		opt:           opt,
		readConnPool:  newConnPool(opt.Read),
		writeConnPool: newConnPool(opt.Write),
	}
}
