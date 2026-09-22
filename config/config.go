// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type RabbitMQClient struct {
	Host       string `config:"host"`
	Port       int    `config:"port"`
	VHost      string `config:"vhost"`
	Username   string `config:"username"`
	Password   string `config:"password"`
	Exchange   string `config:"exchange"`
	Queue      string `config:"queue"`
	RoutingKey string `config:"routing_key"`
}

type MagnumOIDCAuth struct {
	ClientID     string `config:"client_id"`
	ClientSecret string `config:"client_secret"`
	TokenURL     string `config:"token_url"`
}

type MagnumAPI struct {
	Url   string         `config:"url"`
	Limit int            `config:"limit"`
	Auth  MagnumOIDCAuth `config:"auth"`
}

type Mapping struct {
	Nameset string `config:"nameset"`
	Default string `config:"default"`
}

// Correlator configures the beater/correlator engine that groups related
// route log lines into timed event envelopes.
type Correlator struct {
	// OpenOn is the log kind that opens a new envelope: "scheduler",
	// "magnum_subscribe", or "magnum_complete".
	OpenOn string `config:"open_on"`
	// CloseAfter is an envelope's fixed lifetime from its opening log.
	CloseAfter time.Duration `config:"close_after"`
	// SweepInterval is how often expired envelopes are swept and dispatched.
	SweepInterval time.Duration `config:"sweep_interval"`
	// MaxOpen caps simultaneously open envelopes.
	MaxOpen int `config:"max_open"`
	// PendingWait is how long a slab/magrtrsrv log with no matching open
	// envelope waits before it is dropped.
	PendingWait time.Duration `config:"pending_wait"`
}

type Config struct {
	Tags              []string       `config:"tags"`
	PhysicalRouteTags []string       `config:"physical_route_tags"`
	Mapping           *Mapping       `config:"mapping"`
	API               MagnumAPI      `config:"api"`
	RabbitMQClient    RabbitMQClient `config:"rabbitmq_client"`
	Correlator        Correlator     `config:"correlator"`
}

var DefaultConfig = Config{
	Tags:              []string{},
	PhysicalRouteTags: []string{},
	API: MagnumAPI{
		Url:   "https://129.153.131.121/graphql/v1.1",
		Limit: 2000,
		Auth: MagnumOIDCAuth{
			ClientID:     "insite-poller",
			ClientSecret: "QdS1US0v2xABh4d5CliQAWZrmSGPMOxd",
			TokenURL:     "https://129.153.131.121/auth/realms/magnum/protocol/openid-connect/token",
		},
	},
	RabbitMQClient: RabbitMQClient{
		Host:       "localhost",
		Port:       8910,
		VHost:      "/",
		Username:   "insite",
		Password:   "abacus",
		Exchange:   "x.routelogbeat.logs",
		Queue:      "q.routelogbeat.logs",
		RoutingKey: "log.data",
	},
	Correlator: Correlator{
		OpenOn:        "scheduler",
		CloseAfter:    2 * time.Minute,
		SweepInterval: 1 * time.Second,
		MaxOpen:       4096,
		PendingWait:   5 * time.Second,
	},
}
