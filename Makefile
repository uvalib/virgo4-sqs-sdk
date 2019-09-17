GOCMD=go
GOTEST=$(GOCMD) test
PACKAGE=github.com/uvalib/virgo4-sqs-sdk

build: test

test:
	$(GOTEST) -v $(PACKAGE)/awssqs
