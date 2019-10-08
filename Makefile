GOCMD = go
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod
GOFMT = $(GOCMD) fmt
GOVET = $(GOCMD) vet
PACKAGENAME = awssqs

build: test

test:
	cd $(PACKAGENAME); $(GOTEST) -v $(if $(TEST),-run $(TEST),)

dep:
	cd $(PACKAGENAME); $(GOGET) -u
	cd $(PACKAGENAME); $(GOMOD) tidy

fmt:
	cd $(PACKAGENAME); $(GOFMT)

vet:
	cd $(PACKAGENAME); $(GOVET)
