package http

import (
	"testing"
)

func TestUserAgents(t *testing.T) {

	agents, err := UserAgents()

	if err != nil {
		t.Fatalf("Failed to derive user agents, %v", err)
	}

	if len(agents) != 100 {
		t.Fatalf("Unexpected agents length, %d", len(agents))
	}
}

func TestRandomUserAgent(t *testing.T) {

	_, err := RandomUserAgent()

	if err != nil {
		t.Fatalf("Failed to derive random user agent, %v", err)
	}
}
