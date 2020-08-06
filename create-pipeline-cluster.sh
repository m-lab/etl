#!/bin/bash
#
# Configure cluster, network, firewall and node-pools for gardener and etl.

set -x
set -e

PROJECT=${1:?Please provide the GCP project id, e.g. mlab-sandbox}
REGION=${2:?Please provide the cluster region, e.g. us-central1}

gcloud config set project $PROJECT
gcloud config set compute/region $REGION

# The network for comms among the components has to be created first.
if gcloud compute networks list | grep "^data-processing "; then
  echo "Network already exists"
else
  gcloud compute networks create data-processing --subnet-mode=custom \
    --description="Network for communication among backend processing services."
fi

# This allows internal connections between components.
if gcloud compute firewall-rules list | grep "^dp-allow-internal "; then
  echo "Firewall rule dp-allow-internal already exists"
else
  gcloud compute firewall-rules create \
    dp-allow-internal --network=data-processing \
    --allow=tcp:0-65535,udp:0-65535,icmp --direction=INGRESS \
    --source-ranges=10.128.0.0/9,10.100.0.0/16 \
    --description='Allow internal traffic from anywhere'
fi

# Then add the subnet 
# Subnet has the same name and address range across projects, but each is in a distinct (data-processing) VPC network."
if gcloud compute networks subnets list --network=data-processing | grep "^dp-gardener "; then
  echo "subnet data-processing/dp-gardener already exists"
else
  gcloud compute networks subnets create dp-gardener \
    --network=data-processing --range=10.100.0.0/16 \
    --enable-private-ip-google-access \
    --description="Subnet for gardener,etl,annotation-service."
fi

# And define the static IP address that will be used by etl parsers to reach gardener.
gcloud compute addresses create etl-gardener \
  --subnet=dp-gardener --addresses=10.100.1.2

# Now we can create the cluster.
# It includes a default node-pool, though it isn't actually needed?
gcloud container clusters create data-processing \
  --network=data-processing --subnetwork=dp-gardener \
  --enable-autorepair --enable-autoupgrade \
  --scopes=bigquery,taskqueue,compute-rw,storage-ro,service-control,service-management,datastore \
  --num-nodes 2 --image-type=cos --machine-type=n1-standard-4 \
  --node-labels=gardener-node=true --labels=data-processing=true

# Set up node pools for parser and gardener pods.
# Parser needs write access to storage.  Gardener needs only read access.
gcloud container node-pools create parser-pool \
  --cluster=data-processing --num-nodes=3 --machine-type=n1-standard-8 \
  --enable-autorepair --enable-autoupgrade \
  --scopes storage-rw,compute-rw,datastore,cloud-platform \
  --node-labels=parser-node=true \
 # --service-account=etl-k8s-parser@mlab-staging.iam.gserviceaccount.com

gcloud container node-pools create gardener-pool \
--cluster=data-processing --num-nodes=2 --machine-type=n1-standard-2 \
--enable-autorepair --enable-autoupgrade \
--scopes storage-ro,compute-rw,datastore,cloud-platform \
--node-labels=gardener-node=true \
# --service-account=etl-k8s-parser@mlab-staging.iam.gserviceaccount.com

