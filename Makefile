GOCMD=go
GOTEST=$(GOCMD) test

build: test

test:
	$(GOTEST) -v awssqs/...
