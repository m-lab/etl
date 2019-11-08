# Dockerfile to contain the generate_schema_docs CLI.

# Build the command.
FROM golang:1.12 as build
ENV CGO_ENABLED 0
COPY . /go/src/github.com/m-lab/etl
WORKDIR /go/src/github.com/m-lab/etl
RUN go get -v github.com/m-lab/etl/cmd/generate_schema_docs

# Build the image.
FROM alpine:3.7
COPY --from=build /go/bin/generate_schema_docs /
WORKDIR /
ENTRYPOINT ["/generate_schema_docs"]

