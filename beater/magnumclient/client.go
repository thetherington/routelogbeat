package magnumclient

import (
	"net/http"

	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/hasura/go-graphql-client"

	phyTerminals "github.com/thetherington/routelogbeat/beater/magnumclient/queryPhysicalTerminals"
	subTerminals "github.com/thetherington/routelogbeat/beater/magnumclient/querySubcriptionTerminals"
)

type CallbackOptions map[string]string

const (
	CLIENT_TIMEOUT = 10 // time in seconds
)

type Client interface {
	Close()
	RunSubscriptions(done chan struct{})
	QueryTerminals(tag string, limit int, isSub bool, fn func(terminals []Edge, opts ...CallbackOptions) error) error
	QuerySubscriptionTerminals(tag string, limit int, fn func(terminals []subTerminals.Edge, opts ...CallbackOptions) error) error
	QueryPhysicalTerminals(tag string, limit int, fn func(terminals []phyTerminals.Edge, opts ...CallbackOptions) error) error
}

type magnumClient struct {
	httpClient  *http.Client
	queryClient *graphql.Client
	subClient   *graphql.SubscriptionClient
	subIds      []string
}

type ClientCfg struct {
	ApiUrl string
}

func NewMagnumClient(httpClient *http.Client, cfg *ClientCfg) Client {
	subClient := graphql.
		NewSubscriptionClient(getWssURL(cfg.ApiUrl)).
		WithWebSocketOptions(graphql.WebsocketOptions{
			HTTPClient: httpClient,
		}).
		OnError(func(sc *graphql.SubscriptionClient, err error) error {
			logp.Err("subscription client OnError: %v", err)
			return err
		}).
		OnDisconnected(func() {
			logp.Warn("subscription client disconnected")
		}).
		OnConnected(func() {
			logp.Info("subscription client connected")
		}).
		OnSubscriptionComplete(func(sub graphql.Subscription) {
			logp.Info("subcription terminated %s", sub.GetID())
		})

	queryClient := graphql.NewClient(cfg.ApiUrl, httpClient)

	return &magnumClient{
		httpClient:  httpClient,
		queryClient: queryClient,
		subClient:   subClient,
	}
}

func (c *magnumClient) Close() {
	// unsubscribe from all subscriptions
	for _, id := range c.subIds {
		if err := c.subClient.Unsubscribe(id); err != nil {
			logp.Err("error unsubscribing from graphql query with id: %s", id)
		}
	}

	c.subClient.Close()
}

// Run starts the subscription client and the notification checker routines.
func (c *magnumClient) RunSubscriptions(done chan struct{}) {
	go c.subscriptionClientRun(done)
	go c.checkLastNotification(done)
}
