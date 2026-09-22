package magnumclient

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func (c *magnumClient) QueryTerminals(tag string, limit int, fn func(terminals []Edge) error) error {
	// variables
	variables := map[string]any{
		"tag":   strconv.Quote(tag),
		"limit": limit,
	}

	var query QueryTerminals

	err := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), CLIENT_TIMEOUT*time.Second)
		defer cancel()

		err := c.queryClient.Query(ctx, &query, variables)
		if err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return err
	}

	// check if there has been results to process
	if query.Terminals.TotalCount < 1 {
		return fmt.Errorf("Query Results is 0 for Tag: %s, %w", tag, ErrNoTerminals)
	}

	return fn(query.Terminals.Edges)
}
