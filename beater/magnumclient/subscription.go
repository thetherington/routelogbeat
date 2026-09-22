package magnumclient

import (
	"strconv"
	"time"

	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/hasura/go-graphql-client/pkg/jsonutil"
)

var (
	lastNotificationTime time.Time
)

func (c *magnumClient) SubscribeTerminalsUpdated(tag string, fn func(terminals []Edge) error) error {
	// variables
	v := map[string]any{
		"tag": strconv.Quote(tag),
	}

	// subscribe to a query and run a callback function to process the messages
	id, err := c.subClient.Subscribe(SubscriptionTerminalsUpdated{}, v, func(message []byte, err error) error {
		if err != nil {
			return err
		}

		// update the last notification time to now whenever a new notification is received
		lastNotificationTime = time.Now()

		data := SubscriptionTerminalsUpdated{}

		// unmarshal message payload
		if err := jsonutil.UnmarshalGraphQL(message, &data); err != nil {
			logp.Err("failed to unmarshal subscription response for Tag:%s %v", tag, err)
			return nil
		}

		if err := fn(data.TerminalsUpdated); err != nil {
			logp.Err("failed to process terminals updated for Tag:%s %v", tag, err)
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	logp.Info("Subscrition made for Tag: %s with Sub ID: %s", tag, id)

	c.subIds = append(c.subIds, id)

	return nil
}

func (c *magnumClient) subscriptionClientRun(done chan struct{}) {
	for {
		select {
		case <-done:
			logp.Warn("exiting GraphQL subscription client Run() routine")
			return
		default:
		}

		if err := c.subClient.Run(); err != nil {
			logp.Err("subscription client Run error: %v", err)
		}

		if len(c.subClient.GetSubscriptions()) == 0 {
			logp.Warn("no active subscriptions, exiting GraphQL subscription client Run() routine")
			return
		}

		logp.Info("subscription client reconnect/re-run")
	}
}

func (c *magnumClient) checkLastNotification(done chan struct{}) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	// initialize the last notification time to now so that we don't
	// immediately close the subscription client on startup before we receive any notifications
	lastNotificationTime = time.Now()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// if it has been more than 120 minutes since the last notification was received, then close the subscription client
			if time.Since(lastNotificationTime) > 120*time.Minute {
				logp.Info("closing subscription client to test reconnect logic, last notification received at: %s", lastNotificationTime.Format(time.RFC3339))
				c.subClient.Close()

				// reset the last notification time to now after closing the subscription client so
				// that we don't immediately close it again in the next tick before we receive any notifications
				lastNotificationTime = time.Now()
			}
		}
	}
}
