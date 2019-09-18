GOCMD=go
GOTEST=$(GOCMD) test
PACKAGENAME=virgo4-sqs-sdk
PACKAGE=github.com/uvalib/$(PACKAGENAME)

build: test

test:
	$(GOTEST) -v $(PACKAGE)/...
