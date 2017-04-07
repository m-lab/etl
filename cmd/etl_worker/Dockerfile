# The standard google cloud-sdk container that "just works" within GCE.
FROM google/cloud-sdk

# Add the server to respond to HTTP requests at port 8080.
COPY etl_worker /etl_worker
RUN chmod -R a+rx /etl_worker
ENTRYPOINT ["/etl_worker"]
