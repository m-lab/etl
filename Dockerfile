# Dockerfile to contain the generate_schema_docs CLI.

FROM golang:1.14.4-alpine3.12 AS build
RUN apk update
RUN apk add --virtual build-dependencies build-base gcc wget git linux-headers
# Build the command.
COPY . /go/src/github.com/m-lab/etl
WORKDIR /go/src/github.com/m-lab/etl
RUN go get -v github.com/m-lab/etl/cmd/generate_schema_docs

# Now copy the resulting command into the minimal base image.
FROM alpine:3.12
COPY --from=build /go/bin/generate_schema_docs /
COPY --from=build /go/src/github.com/m-lab/etl/schema/descriptions /schema/descriptions/
WORKDIR /
ENTRYPOINT ["/generate_schema_docs"]

