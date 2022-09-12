FROM golang:1.18-alpine

ENV GOPATH /go

COPY go.mod /go/src/logzio-salesforce-logs-receiver/
COPY go.sum /go/src/logzio-salesforce-logs-receiver/
COPY *.go /go/src/logzio-salesforce-logs-receiver/

WORKDIR /go/src/app

COPY ./docker/go.build.mod ./go.mod
COPY ./docker/go.sum ./
COPY ./docker/*.go ./

RUN go mod download
RUN go build -o ./logzio-salesforce-collector

CMD ["./logzio-salesforce-collector"]
