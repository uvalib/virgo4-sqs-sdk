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

check:
	go get honnef.co/go/tools/cmd/staticcheck
	cd $(PACKAGENAME); ~/go/bin/staticcheck -checks all,-ST1000,-S1002,-ST1003,-ST1020,-ST1021,-ST1022 *.go
