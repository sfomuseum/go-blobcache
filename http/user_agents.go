package http

import (
	_ "embed"
	"encoding/json"
	"log/slog"
	"math/rand"
	net_http "net/http"
	"sync"
	"time"
)

// https://github.com/microlinkhq/top-user-agents/blob/master/src/index.json

//go:embed user_agents.json
var user_agents_json []byte

var user_agents = sync.OnceValues(func() ([]string, error) {

	var agents []string

	err := json.Unmarshal(user_agents_json, &agents)
	return agents, err
})

// UserAgents returns the full list of user‑agent strings that were
// embedded in the binary. The function may return an error if the
// embedded JSON could not be decoded.
func UserAgents() ([]string, error) {
	return user_agents()
}

// RandomUserAgent selects a random user‑agent string from the embedded
// list and returns it.  If the list cannot be loaded, an error is
// returned.
func RandomUserAgent() (string, error) {

	agents, err := user_agents()

	if err != nil {
		return "", err
	}

	rand.Seed(time.Now().UnixNano())
	return agents[rand.Intn(len(agents))], nil
}

// AssignRandomUserAgent sets the "User-Agent" header of the supplied
// *net/http.Request to a randomly chosen user‑agent string.
func AssignRandomUserAgent(req *net_http.Request) error {

	ua, err := RandomUserAgent()

	if err != nil {
		return err
	}

	slog.Debug("SET", "ua", ua)
	req.Header.Set("User-Agent", ua)
	return nil
}
