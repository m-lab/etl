# The standard google cloud-sdk container that "just works" within GCE.
FROM google/cloud-sdk

# Add the server to respond to HTTP requests at port 8080.
COPY worker /etl-worker
RUN chmod -R a+rx /etl-worker
ENTRYPOINT ["/etl-worker"]
