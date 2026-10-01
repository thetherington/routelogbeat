package magnumclient

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func (c *magnumClient) QueryTerminals(tag string, limit int, isSub bool, fn func(terminals []Edge, opts ...CallbackOptions) error) error {
	// variables
	variables := map[string]any{
		"tag":    strconv.Quote(tag),
		"limit":  limit,
		"offset": 0,
		"isSub":  isSub,
	}

	for offset := 0; ; offset += limit {
		variables["offset"] = offset

		var query QueryTerminals

		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), CLIENT_TIMEOUT*time.Second)
			defer cancel()

			return c.queryClient.Query(ctx, &query, variables)
		}()
		if err != nil {
			return err
		}

		// check if there has been results to process
		if query.Terminals.TotalCount < 1 {
			return fmt.Errorf("Query Results is 0 for Tag: %s, %w", tag, ErrNoTerminals)
		}

		if err := fn(query.Terminals.Edges, map[string]string{"tag": tag}); err != nil {
			return err
		}

		if offset+limit >= query.Terminals.TotalCount {
			return nil
		}
	}
}
