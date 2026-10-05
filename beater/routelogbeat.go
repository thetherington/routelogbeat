package beater

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"

	"github.com/thetherington/routelogbeat/beater/cache"
	"github.com/thetherington/routelogbeat/beater/correlator"
	"github.com/thetherington/routelogbeat/beater/httpclient"
	"github.com/thetherington/routelogbeat/beater/magnumclient"
	"github.com/thetherington/routelogbeat/beater/rabbitmqclient"
	routelogCfg "github.com/thetherington/routelogbeat/config"
)

var (
	SlabMap        = cache.NewCacheMap[string, Slabs](0)
	DestinationMap = cache.NewCacheMap[string, Terminal](0)
	SourceMap      = cache.NewCacheMap[string, Terminal](0)
)

// routelogbeat configuration.
type routelogbeat struct {
	done           chan struct{}
	config         routelogCfg.Config
	client         beat.Client
	rabbitmqClient *rabbitmqclient.Client
	engine         *correlator.Engine
	magnumClient   magnumclient.Client
}

// New creates an instance of routelogbeat.
func New(b *beat.Beat, cfg *config.C) (beat.Beater, error) {
	c := routelogCfg.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	// Validate there is atleast 1 destination tag
	if len(c.Destinations) < 1 || len(c.Sources) < 1 {
		return nil, errors.New("beat requires atleast 1 destination and 1 source tag in the configuration")
	}

	// Validate if mapping is enabled then the nameset is not blank
	if c.Mapping != nil && c.Mapping.Nameset == "" {
		return nil, errors.New("nameset cannot be blank if mapping is enabled")
	}

	done := make(chan struct{})

	// create generic http client interface and authenticate with magnum
	// http client contains a cookieJar that is updated by a goroutine
	httpClient, err := httpclient.NewHTTPClient(&httpclient.MagnumAuthCredentials{
		ClientID:     c.API.Auth.ClientID,
		ClientSecret: c.API.Auth.ClientSecret,
		TokenURL:     c.API.Auth.TokenURL,
		Done:         done,
	})
	if err != nil {
		return nil, fmt.Errorf("error authenticating with magnum: %v", err)
	}

	// create magnum client
	magnumClient := magnumclient.NewMagnumClient(httpClient, &magnumclient.ClientCfg{
		ApiUrl: c.API.Url,
	})

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

	// Parse and validate the correlator's "open_on" configuration.
	openOn, ok := correlator.ParseKind(c.Correlator.OpenOn)
	if !ok || openOn == correlator.KindSlab || openOn == correlator.KindMagrtrsrv {
		return nil, fmt.Errorf("invalid correlator.open_on: %q", c.Correlator.OpenOn)
	}

	// Set up the resolver for the correlator using the SlabMapResolver function.
	resolver := correlator.ResolverFunc(SlabMapResolver)

	// setup the metadata resolver for the correlator
	metadata := correlator.MetadataResolverFunc(TerminalMapResolver)

	engine, err := correlator.New(correlator.Config{
		OpenOn:             openOn,
		CloseAfter:         c.Correlator.CloseAfter,
		SweepInterval:      c.Correlator.SweepInterval,
		MaxOpen:            c.Correlator.MaxOpen,
		PendingWait:        c.Correlator.PendingWait,
		RequireDstMetadata: c.Correlator.RequireDstMetadata,
		CloseOnComplete:    c.Correlator.CloseOnComplete,
		CompleteGrace:      c.Correlator.CompleteGrace,
	}, correlatorOptions(resolver, metadata)...)
	if err != nil {
		return nil, fmt.Errorf("correlator: %w", err)
	}

	bt := &routelogbeat{
		done:           done,
		config:         c,
		rabbitmqClient: rabbitmqclient.NewClient(rbcfg),
		engine:         engine,
		magnumClient:   magnumClient,
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

	// Query the terminals with PhysicalRouteTags from the magnum client before entering the main loop.
	err = bt.magnumClient.QueryTerminals(bt.config.PhysicalRouteTags[0], bt.config.API.Limit, false, bt.ProcessSlabTerminal)
	if err != nil {
		return err
	}

	// Query the terminals for Destinations tags
	for _, tag := range bt.config.Destinations {
		err = bt.magnumClient.QueryTerminals(tag, bt.config.API.Limit, true, bt.ProcessSubTerminal)
		if err != nil {
			return err
		}
	}

	// Query the terminals for Sources tags
	for _, tag := range bt.config.Sources {
		err = bt.magnumClient.QueryTerminals(tag, bt.config.API.Limit, true, bt.ProcessSubTerminal)
		if err != nil {
			return err
		}
	}

	// Start a goroutine to periodically log correlator engine statistics.
	go bt.CorrelatorEngineStats()

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

	// The magclientsrv client address, when a magclientsrv log was correlated.
	client := "n/a"
	if env.Resolved.ClientIP != "" {
		client = env.Resolved.ClientIP
		if env.Resolved.ClientPort != 0 {
			client = net.JoinHostPort(env.Resolved.ClientIP, strconv.Itoa(env.Resolved.ClientPort))
		}
	}

	// Log the basic information about the closed envelope.
	logp.Info("routelogbeat: envelope closed src=%s dst=%s opened_by=%s partial=%t "+
		"logs=%d sources=%v duration=%s scheduler_to_slab=%s client=%s reason=%s",
		env.Key.Src, env.Key.Dst, env.OpenedBy.Family(), env.Partial,
		len(env.Records), env.SourceCounts(), env.Duration(), schedToSlab, client, env.Reason)

	// print the env.Records
	// for _, record := range env.Records {
	// 	logp.Info("routelogbeat: record=%v", record)
	// }

	// print the resolved slabs from the envelope's records
	logp.Info("routelogbeat: resolved slabs from envelope's records %v", env.Resolved.Slabs)

	// print the source and destination metadata resolved for the envelope's Key.
	// Either side is nil if no MetadataResolver is configured or it had nothing
	// for that UUID.
	logp.Info("routelogbeat: resolved metadata src=%v dst=%v", env.Resolved.SrcMeta, env.Resolved.DstMeta)
}

// Stop stops routelogbeat.
func (bt *routelogbeat) Stop() {
	bt.rabbitmqClient.Close() // stop input first
	bt.engine.Close()         // then the correlation engine
	bt.magnumClient.Close()   // close the magnum client

	bt.client.Close()
	close(bt.done)
}

func (bt *routelogbeat) CorrelatorEngineStats() {
	statsTicker := time.NewTicker(time.Minute)
	defer statsTicker.Stop()

	for {
		select {
		case <-bt.done:
			bt.logStats() // final totals on shutdown
			return

		case <-statsTicker.C:
			bt.logStats()
		}
	}
}

func (bt *routelogbeat) logStats() {
	s := bt.engine.Stats()
	logp.Info("correlator: submitted=%d discarded=%d parse_errors=%d unresolved_slab=%d "+
		"(no_slab_match=%d multicast_conflict=%d) pending_dropped=%d "+
		"(no_slab_match=%d multicast_conflict=%d) filtered_no_dst_metadata=%d envelopes_opened=%d envelopes_closed=%d "+
		"(complete=%d) open=%d max_open_evictions=%d",
		s.Submitted, s.Discarded, s.ParseErrors, s.UnresolvedSlab,
		s.UnresolvedNoSlabMatch, s.UnresolvedMulticastConflict, s.PendingDropped,
		s.DroppedNoSlabMatch, s.DroppedMulticastConflict, s.FilteredNoDstMetadata, s.EnvelopesOpened, s.EnvelopesClosed,
		s.ClosedComplete, s.EnvelopesOpened-s.EnvelopesClosed, s.MaxOpenEvictions)
}
