package rabbitmqclient

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/elastic-agent-libs/logp"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultHost         = "localhost"
	defaultPort         = 8910
	defaultVHost        = "/"
	defaultUser         = "insite"
	defaultPassword     = "abacus"
	defaultExchange     = "x.routelogbeat.logs"
	defaultExchangeType = "direct"
	defaultQueue        = "q.routelogbeat.logs"
	defaultRoutingKey   = "log.data"
	defaultDurable      = true
	defaultPersistent   = true
	defaultReconnectInt = 5 * time.Second
)

// Config contains RabbitMQ connection and routing options.
type Config struct {
	Host         string
	Port         int
	VHost        string
	User         string
	Password     string
	Exchange     string
	ExchangeType string
	Queue        string
	RoutingKey   string
	Durable      bool
	Persistent   bool

	// ReconnectInterval controls how often the client retries when RabbitMQ is unavailable.
	ReconnectInterval time.Duration
}

// Client owns the RabbitMQ connection and channels used for setup and consume operations.
type Client struct {
	mu sync.RWMutex

	cfg Config
	uri string

	conn       *amqp.Connection
	setupCh    *amqp.Channel
	consumeCh  *amqp.Channel
	queueName  string
	routingKey string
	persistent bool

	closed chan struct{}
	once   sync.Once
}

// DefaultConfig returns the opinionated defaults for routelogbeat queues.
func DefaultConfig() Config {
	return Config{
		Host:              defaultHost,
		Port:              defaultPort,
		VHost:             defaultVHost,
		User:              defaultUser,
		Password:          defaultPassword,
		Exchange:          defaultExchange,
		ExchangeType:      defaultExchangeType,
		Queue:             defaultQueue,
		RoutingKey:        defaultRoutingKey,
		Durable:           defaultDurable,
		Persistent:        defaultPersistent,
		ReconnectInterval: defaultReconnectInt,
	}
}

// NewDefaultClient creates a client using routelogbeat default RabbitMQ settings.
func NewDefaultClient() *Client {
	return NewClient(DefaultConfig())
}

// NewClient connects to RabbitMQ, declares the exchange and queue, binds the queue to the routing key, and prepares channels.
func NewClient(cfg Config) *Client {
	cfg = applyDefaults(cfg)

	uri := amqp.URI{
		Scheme:   "amqp",
		Host:     cfg.Host,
		Port:     cfg.Port,
		Username: cfg.User,
		Password: cfg.Password,
		Vhost:    cfg.VHost,
	}

	c := &Client{
		cfg:        cfg,
		uri:        uri.String(),
		routingKey: cfg.RoutingKey,
		persistent: cfg.Persistent,
		closed:     make(chan struct{}),
	}

	go c.connectWithRetry()
	go c.reconnectLoop()

	return c
}

// Consume starts a consumer stream from the configured queue.
func (c *Client) Consume(consumerTag string, autoAck bool) (<-chan amqp.Delivery, error) {
	out := make(chan amqp.Delivery)

	go c.consumeLoop(out, consumerTag, autoAck)

	return out, nil
}

// consumeLoop maintains a live consumer stream and transparently resubscribes after reconnects.
func (c *Client) consumeLoop(out chan<- amqp.Delivery, consumerTag string, autoAck bool) {
	defer close(out)

	for {
		// Wait until a usable consume channel is available.
		ch, queueName, err := c.getConsumeChannel()
		if err != nil {
			return
		}

		// Create a consumer stream on the current queue/channel.
		deliveries, consumeErr := ch.Consume(
			queueName,
			consumerTag,
			autoAck,
			false,
			false,
			false,
			nil,
		)
		if consumeErr != nil {
			// Channel may be mid-reconnect; wait and retry subscribe.
			if !c.waitForReconnectInterval() {
				return
			}

			continue
		}

		for {
			select {
			case <-c.closed:
				return
			case d, ok := <-deliveries:
				if !ok {
					// Delivery channel was closed (often due to broker restart); resubscribe.
					if !c.waitForReconnectInterval() {
						return
					}

					goto resubscribe
				}

				select {
				// Forward messages to callers on a stable output channel.
				case out <- d:
				case <-c.closed:
					return
				}
			}
		}

	resubscribe:
	}
}

// DeliveryMode returns the AMQP delivery mode from persistent config.
func (c *Client) DeliveryMode() uint8 {
	if c.persistent {
		return amqp.Persistent
	}

	return amqp.Transient
}

// Close releases RabbitMQ resources.
func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})

	c.mu.Lock()
	consumeCh := c.consumeCh
	setupCh := c.setupCh
	conn := c.conn
	c.consumeCh = nil
	c.setupCh = nil
	c.conn = nil
	c.mu.Unlock()

	var err error
	err = firstErr(err, closeChannel(consumeCh))
	err = firstErr(err, closeChannel(setupCh))
	err = firstErr(err, closeConnection(conn))

	return err
}

// applyDefaults fills in empty config fields with package defaults.
func applyDefaults(cfg Config) Config {
	defaults := DefaultConfig()

	if cfg.Host == "" {
		cfg.Host = defaults.Host
	}
	if cfg.Port == 0 {
		cfg.Port = defaults.Port
	}
	if cfg.VHost == "" {
		cfg.VHost = defaults.VHost
	}
	if cfg.User == "" {
		cfg.User = defaults.User
	}
	if cfg.Password == "" {
		cfg.Password = defaults.Password
	}
	if cfg.Exchange == "" {
		cfg.Exchange = defaults.Exchange
	}
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = defaults.ExchangeType
	}
	if cfg.Queue == "" {
		cfg.Queue = defaults.Queue
	}
	if cfg.RoutingKey == "" {
		cfg.RoutingKey = defaults.RoutingKey
	}
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = defaults.ReconnectInterval
	}

	return cfg
}

/*
RabbitMQ client - connection & reconnection flow
================================================

The client runs three long-lived goroutines. They never call one another
directly; they coordinate through shared state guarded by c.mu:

    c.conn       - live *amqp.Connection      | published by connectOnce()
    c.consumeCh  - live consume *amqp.Channel  | published by connectOnce()
    c.queueName  - name of the declared queue  | published by connectOnce()
    c.closed     - closed once by Close(); every wait and select below
                   unblocks and the goroutines return

Every retry sleep goes through waitForReconnectInterval(), which returns
false as soon as c.closed is closed, so a single Close() call stops all
three goroutines and every helper that is mid-wait.


GOROUTINE 1 - initial connect                    (go, from NewClient)
--------------------------------------------------------------------
    connectWithRetry()
        |  retry loop: until connectOnce() succeeds or Close()
        v
    connectOnce()
        |  amqp.Dial(uri)                    -> conn
        |  conn.Channel() x2                 -> setupCh, consumeCh
        |  setupCh.ExchangeDeclare(...)
        |  setupCh.QueueDeclare(...)         -> q
        |  setupCh.QueueBind(q.Name, ...)
        |  c.mu: publish conn / consumeCh / queueName,
        |        then close the PREVIOUS conn + channels
        |
        +-- err -> waitForReconnectInterval() -> retry  (or return on Close)
        +-- ok  -> return; goroutine exits


GOROUTINE 2 - supervise & reconnect              (go, from NewClient)
--------------------------------------------------------------------
    reconnectLoop()
        |
        v
    getConnection() ........ waitForReconnectInterval() loop until
        |                    c.conn is non-nil and open; err on Close()
        v
    conn.NotifyClose(notifyClose)
        |  block on:
        |    <-c.closed     -> return                    (Close called)
        |    <-notifyClose  -> broker / connection dropped
        v
    connectWithRetry() -> connectOnce()   (same chain as goroutine 1;
        |                                  re-publishes c.conn etc.)
        +-- err -> return                                 (Close called)
        +-- ok  -> log "reconnected"; loop back to getConnection()


GOROUTINE 3 - consumer stream                    (go, per Consume() call)
--------------------------------------------------------------------
    Consume(tag, autoAck)
        |  makes the stable output channel `out`, returns it to the caller
        v
    consumeLoop(out, tag, autoAck)                  [defer close(out)]
        |
        v
    getConsumeChannel() .... waits until c.consumeCh is non-nil;
        |                     returns (ch, queueName); err on Close()
        v
    ch.Consume(queueName, tag, autoAck, ...)  -> deliveries
        |
        +-- subscribe err -> waitForReconnectInterval() -> restart loop
        |
        v
    forward loop:
        <-c.closed              -> return
        d, ok := <-deliveries:
            ok   -> out <- d                       (or return on <-c.closed)
            !ok  -> deliveries closed because connectOnce() swapped in a
                    new consumeCh and closed the old one ->
                    waitForReconnectInterval() -> goto resubscribe ->
                    back to getConsumeChannel(), which now returns the
                    freshly published c.consumeCh


HOW A RECONNECT REACHES THE CONSUMER
--------------------------------------------------------------------
    broker drops
      -> notifyClose fires in reconnectLoop()
      -> connectWithRetry() -> connectOnce()
           -> opens new conn + channels
           -> c.mu: publishes new c.consumeCh / c.queueName
           -> closes the OLD conn + channels
      -> old `deliveries` channel closes in consumeLoop()   (ok == false)
      -> consumeLoop() resubscribes via getConsumeChannel()
           -> receives the NEW c.consumeCh, calls ch.Consume() again
    `out` never closes during this, so callers see one uninterrupted
    stream (minus any messages in flight during the outage).


SHUTDOWN
--------------------------------------------------------------------
    Close()
      -> c.once: close(c.closed)
      -> every waitForReconnectInterval() / select returns / exits:
           connectWithRetry, reconnectLoop, getConnection,
           consumeLoop, getConsumeChannel
      -> c.mu: detach conn / setupCh / consumeCh, then close each
           (closeChannel / closeConnection ignore amqp.ErrClosed)
      -> consumeLoop()'s deferred close(out) closes the caller's channel

Notes:
- connectWithRetry() serves both the first connect and every later reconnect.
- Only connectOnce() writes c.conn / c.consumeCh / c.queueName; every other
  function reads them under c.mu and waits when they are not ready yet.
- consumeLoop() keeps `out` stable while the internal subscription is rebuilt.
*/

// reconnectLoop blocks on broker disconnect notifications and reconnects until the client is closed.
func (c *Client) reconnectLoop() {
	for {
		conn, err := c.getConnection()
		if err != nil {
			return
		}

		notifyClose := make(chan *amqp.Error, 1)
		conn.NotifyClose(notifyClose)

		select {
		case <-c.closed:
			return
		case amqpErr := <-notifyClose:
			if amqpErr != nil {
				logp.Warn("rabbitmq disconnected: %v", amqpErr)
			} else {
				logp.Warn("rabbitmq disconnected")
			}

			if err := c.connectWithRetry(); err != nil {
				return
			}

			logp.Info("rabbitmq reconnected")
		}
	}
}

// connectWithRetry keeps attempting to establish topology until success or client shutdown.
func (c *Client) connectWithRetry() error {
	for {
		if err := c.connectOnce(); err == nil {
			logp.Info("rabbitmq connection established")
			return nil
		} else {
			logp.Warn("rabbitmq connect/reconnect failed, retrying in %s: %v", c.cfg.ReconnectInterval, err)
		}

		if !c.waitForReconnectInterval() {
			return errors.New("rabbitmq client is closed")
		}
	}
}

// connectOnce performs one full connect cycle: dial, channels, exchange, queue, and binding.
func (c *Client) connectOnce() error {
	conn, err := amqp.Dial(c.uri)
	if err != nil {
		return fmt.Errorf("rabbitmq connect failed: %w", err)
	}

	setupCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("rabbitmq setup channel failed: %w", err)
	}

	consumeCh, err := conn.Channel()
	if err != nil {
		_ = setupCh.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq consume channel failed: %w", err)
	}

	if err = setupCh.ExchangeDeclare(
		c.cfg.Exchange,
		c.cfg.ExchangeType,
		c.cfg.Durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		_ = consumeCh.Close()
		_ = setupCh.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq exchange declare failed: %w", err)
	}

	q, err := setupCh.QueueDeclare(
		c.cfg.Queue,
		c.cfg.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = consumeCh.Close()
		_ = setupCh.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq queue declare failed: %w", err)
	}

	if err = setupCh.QueueBind(
		q.Name,
		c.cfg.RoutingKey,
		c.cfg.Exchange,
		false,
		nil,
	); err != nil {
		_ = consumeCh.Close()
		_ = setupCh.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq queue bind failed: %w", err)
	}

	c.mu.Lock()
	oldConsumeCh := c.consumeCh
	oldSetupCh := c.setupCh
	oldConn := c.conn

	c.conn = conn
	c.setupCh = setupCh
	c.consumeCh = consumeCh
	c.queueName = q.Name
	c.mu.Unlock()

	_ = closeChannel(oldConsumeCh)
	_ = closeChannel(oldSetupCh)
	_ = closeConnection(oldConn)

	return nil
}

// getConnection waits for an active connection or returns when the client is closed.
func (c *Client) getConnection() (*amqp.Connection, error) {
	for {
		select {
		case <-c.closed:
			return nil, errors.New("rabbitmq client is closed")
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}

		if !c.waitForReconnectInterval() {
			return nil, errors.New("rabbitmq client is closed")
		}
	}
}

// getConsumeChannel waits for a usable consume channel and queue name pair.
func (c *Client) getConsumeChannel() (*amqp.Channel, string, error) {
	for {
		select {
		case <-c.closed:
			return nil, "", errors.New("rabbitmq client is closed")
		default:
		}

		c.mu.RLock()
		consumeCh := c.consumeCh
		queueName := c.queueName
		c.mu.RUnlock()

		if consumeCh != nil {
			return consumeCh, queueName, nil
		}

		if !c.waitForReconnectInterval() {
			return nil, "", errors.New("rabbitmq client is closed")
		}
	}
}

// waitForReconnectInterval sleeps for the configured retry interval and aborts on shutdown.
func (c *Client) waitForReconnectInterval() bool {
	t := time.NewTimer(c.cfg.ReconnectInterval)
	defer t.Stop()

	select {
	case <-c.closed:
		return false
	case <-t.C:
		return true
	}
}

// closeChannel closes an AMQP channel and ignores "already closed" errors.
func closeChannel(ch *amqp.Channel) error {
	if ch == nil {
		return nil
	}

	if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return err
	}

	return nil
}

// closeConnection closes an AMQP connection and ignores "already closed" errors.
func closeConnection(conn *amqp.Connection) error {
	if conn == nil {
		return nil
	}

	if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return err
	}

	return nil
}

// firstErr returns the first non-nil error across a sequence of close operations.
func firstErr(currentErr, nextErr error) error {
	if currentErr != nil {
		return currentErr
	}

	return nextErr
}
