package magnumclient

import "net/url"

// converts a url to websocket scheme url
func getWssURL(s string) string {
	// replace the "https" in the api url with "wss"
	u, _ := url.Parse(s)
	u.Scheme = "wss"

	return u.String()
}
