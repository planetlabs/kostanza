FROM golang:1.14-alpine3.11 AS build
WORKDIR /go/src/github.com/planetlabs/kostanza

# Build indepdently for caching purposes.
ADD vendor vendor
RUN go build -i ./vendor/...
# Build our binaries.
ADD . .
RUN go install ./...

FROM alpine:3.8
RUN apk update && apk add ca-certificates
COPY --from=build /go/bin/kostanza /usr/local/bin/kostanza
