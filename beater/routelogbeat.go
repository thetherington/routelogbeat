package beater

import (
	"encoding/json"
	"fmt"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"

	"github.com/thetherington/routelogbeat/beater/correlator"
	"github.com/thetherington/routelogbeat/beater/rabbitmqclient"
	routelogCfg "github.com/thetherington/routelogbeat/config"
)

// routelogbeat configuration.
type routelogbeat struct {
	done           chan struct{}
	config         routelogCfg.Config
	client         beat.Client
	rabbitmqClient *rabbitmqclient.Client
	engine         *correlator.Engine
	resolver       *correlator.MapResolver
}

// New creates an instance of routelogbeat.
func New(b *beat.Beat, cfg *config.C) (beat.Beater, error) {
	c := routelogCfg.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	rbcfg := rabbitmqclient.Config{
		Host:              c.RabbitMQClient.Host,
		Port:              c.RabbitMQClient.Port,
		VHost:             c.RabbitMQClient.VHost,
		User:              c.RabbitMQClient.Username,
		Password:          c.RabbitMQClient.Password,
		Exchange:          c.RabbitMQClient.Exchange,
		ExchangeType:      rabbitmqclient.DefaultConfig().ExchangeType,
		Queue:             c.RabbitMQClient.Queue,
		RoutingKey:        c.RabbitMQClient.RoutingKey,
		Durable:           rabbitmqclient.DefaultConfig().Durable,
		Persistent:        rabbitmqclient.DefaultConfig().Persistent,
		ReconnectInterval: rabbitmqclient.DefaultConfig().ReconnectInterval,
	}

	openOn, ok := correlator.ParseKind(c.Correlator.OpenOn)
	if !ok || openOn == correlator.KindSlab || openOn == correlator.KindMagrtrsrv {
		return nil, fmt.Errorf("invalid correlator.open_on: %q", c.Correlator.OpenOn)
	}

	// resolver is populated by the main program from wherever it learns
	// route/destination -> slab facts; that source is not wired up yet
	// (Phase 1 of the correlation engine — see the design notes).
	resolver := correlator.NewMapResolver()
	engine, err := correlator.New(correlator.Config{
		OpenOn:        openOn,
		CloseAfter:    c.Correlator.CloseAfter,
		SweepInterval: c.Correlator.SweepInterval,
		MaxOpen:       c.Correlator.MaxOpen,
		PendingWait:   c.Correlator.PendingWait,
	}, correlator.WithResolver(resolver), correlator.WithClock(correlator.RealClock{}))
	if err != nil {
		return nil, fmt.Errorf("correlator: %w", err)
	}

	bt := &routelogbeat{
		done:           make(chan struct{}),
		config:         c,
		rabbitmqClient: rabbitmqclient.NewClient(rbcfg),
		engine:         engine,
		resolver:       resolver,
	}

	return bt, nil
}

// Run starts routelogbeat.
func (bt *routelogbeat) Run(b *beat.Beat) error {
	logp.Info("routelogbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	logsCh, err := bt.rabbitmqClient.Consume(b.Info.Beat, true)
	if err != nil {
		return err
	}

	for {
		select {
		case <-bt.done:
			// Stop has already closed bt.engine; keep draining Events() until
			// it closes so the actor's final shutdown sends (any envelopes
			// still open at Close time, dispatched with ReasonShutdown)
			// always complete instead of blocking forever with no reader.
			for env := range bt.engine.Events() {
				bt.logEnvelope(env)
			}
			return nil

		case env := <-bt.engine.Events():
			// Phase 1 placeholder: log the closed envelope. Phase 2 replaces
			// this with bt.client.Publish(envelopeToEvent(env)) once the
			// presentation shape is decided.
			bt.logEnvelope(env)

		case l := <-logsCh:
			// decode the log message from the RabbitMQ message body as json
			var logMessage SyslogMessage
			if err := json.Unmarshal(l.Body, &logMessage); err != nil {
				logp.Err("Failed to decode log message: %v", err)
				continue
			}

			if raw, ok := toRawLog(&logMessage); ok {
				bt.engine.Submit(raw) // the engine matches it against a parser, or discards it
			}
		}
	}
}

// logEnvelope reports one closed envelope. It is the Phase 1 stand-in for
// publishing a beat.Event (see Run).
func (bt *routelogbeat) logEnvelope(env *correlator.Envelope) {
	if env == nil {
		return
	}

	schedToSlab := "n/a"
	if ms, ok := env.SchedulerToSlabMillis(); ok {
		schedToSlab = fmt.Sprintf("%dms", ms)
	}

	logp.Info("routelogbeat: envelope closed src=%s dst=%s opened_by=%s partial=%t "+
		"logs=%d sources=%v duration=%s scheduler_to_slab=%s reason=%s",
		env.Key.Src, env.Key.Dst, env.OpenedBy.Family(), env.Partial,
		len(env.Records), env.SourceCounts(), env.Duration(), schedToSlab, env.Reason)
}

// Stop stops routelogbeat.
func (bt *routelogbeat) Stop() {
	bt.rabbitmqClient.Close() // stop input first
	bt.engine.Close()         // then the correlation engine

	bt.client.Close()
	close(bt.done)
}
