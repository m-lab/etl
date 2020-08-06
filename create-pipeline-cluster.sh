#!/bin/bash

PROJECT=mlab-oti
REGION=us-central1

set -x
set -e

# The network for comms among the components has to be created first.
gcloud --project=$PROJECT \
  compute networks create data-processing --subnet-mode=custom \
  --description="Network for communication among backend processing services."

gcloud --project=$PROJECT compute firewall-rules create dp-allow-ssh \
  --network=data-processing --allow=tcp:22 --direction=INGRESS \
  --description='Allow SSH from anywhere'

# This allows internal connections between components.
gcloud --project=$PROJECT compute firewall-rules create \
  dp-allow-internal --network=data-processing \
  --allow=tcp:0-65535,udp:0-65535,icmp --direction=INGRESS \
  --source-ranges=10.128.0.0/9,10.100.0.0/16 \
  --description='Allow internal traffic from anywhere'

# Then add the subnet 
gcloud --project=$PROJECT \
  compute networks subnets create dp-gardener \
  --network=data-processing --range=10.100.0.0/16 \
  --enable-private-ip-google-access --region=$REGION \
  --description="Subnet for gardener,etl,annotation-service. Subnet has the same name and address range across projects, but each is in a distinct (data-processing) VPC network."

# And define the static IP address that will be used by etl parsers to reach gardener.
gcloud --project=$PROJECT compute addresses create etl-gardener \
  --region=$REGION --subnet=dp-gardener --addresses=10.100.1.2

# Now we can create the cluster.
# It includes a default node-pool, though it isn't actually needed?
gcloud --project=$PROJECT container clusters create data-processing \
  --region=$REGION --enable-autorepair --enable-autoupgrade \
  --network=data-processing --subnetwork=dp-gardener \
  --scopes=bigquery,taskqueue,compute-rw,storage-ro,service-control,service-management,datastore \
  --num-nodes 2 --image-type=cos --machine-type=n1-standard-4 \
  --node-labels=gardener-node=true --labels=data-processing=true

# Set up node pools for parser and gardener pods.
# Parser needs write access to storage.  Gardener needs only read access.
# Parser also need read access to archive-measurement-lab, which is accomplished
# by granting access to the appropriate service-account.
gcloud --project=$PROJECT container node-pools create parser-pool --cluster=data-processing \
--num-nodes=3 --region=$REGION --scopes storage-rw,compute-rw,datastore,cloud-platform \
--node-labels=parser-node=true --enable-autorepair --enable-autoupgrade \
--machine-type=n1-standard-8 # --service-account=etl-k8s-parser@mlab-staging.iam.gserviceaccount.com

gcloud --project=$PROJECT container node-pools create gardener-pool --cluster=data-processing \
--num-nodes=2 --region=$REGION --scopes storage-ro,compute-rw,datastore,cloud-platform \
--node-labels=gardener-node=true --enable-autorepair --enable-autoupgrade \
--machine-type=n1-standard-2 # --service-account=etl-k8s-parser@mlab-staging.iam.gserviceaccount.com

