SSF := ../..
.PHONY: build test race vet linux image
build:
	cd $(SSF) && go build -o adapter/agentenvadapter/bin/agentenvadapter ./adapter/agentenvadapter/cmd/agentenvadapter
test:
	cd $(SSF) && go test ./adapter/agentenvadapter/...
race:
	cd $(SSF) && go test -race ./adapter/agentenvadapter/...
vet:
	cd $(SSF) && go vet ./adapter/agentenvadapter/...
linux:
	cd $(SSF) && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o adapter/agentenvadapter/bin/agentenvadapter-linux ./adapter/agentenvadapter/cmd/agentenvadapter
image:
	cd $(SSF) && docker build -f adapter/agentenvadapter/Dockerfile -t agentenvadapter:0.1.0 .
