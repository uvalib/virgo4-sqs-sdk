GOCMD=go
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
PACKAGENAME=awssqs

build: test

test:
	cd $(PACKAGENAME); $(GOTEST) -v $(if $(TEST),-run $(TEST),)

deps:
	cd $(PACKAGENAME); $(GOGET) -u
	cd $(PACKAGENAME); $(GOMOD) tidy
