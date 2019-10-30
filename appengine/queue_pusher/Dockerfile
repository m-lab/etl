FROM golang:1.12 as build

ADD . /go/src/github.com/m-lab/etl/appengine/queue_pusher
RUN go get
# Add the server to respond to HTTP requests at port 8080.
COPY --from=build /go/bin/queue_pusher /
COPY queue_pusher /queue_pusher
RUN chmod -R a+rx /queue_pusher
ENTRYPOINT ["/queue_pusher"]
