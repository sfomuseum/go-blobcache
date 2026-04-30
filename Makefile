GOMOD=$(shell test -f "go.work" && echo "readonly" || echo "vendor")
LDFLAGS=-s -w

vuln:
	govulncheck -show verbose ./...

cli:
	go build -tags=$(TAGS) -mod $(GOMOD) -ldflags="$(LDFLAGS)" -o bin/blobcache cmd/blobcache/main.go

cli-local:
	@make cli TAGS=no_gcs,no_azure,no_s3
