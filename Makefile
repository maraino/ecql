PACKAGE=github.com/maraino/ecql

all:
	go build $(PACKAGE)

test:
	go test -cover $(PACKAGE)

cover:
	go test -coverprofile=c.out $(PACKAGE)
	go tool cover -html=c.out

integrate:
	go test -cover -tags=integration $(PACKAGE)

integrate-cover:
	go test -coverprofile=c.out -tags=integration -v $(PACKAGE)
	go tool cover -html=c.out
