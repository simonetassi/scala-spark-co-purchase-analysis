#!/bin/sh

num_workers=$1

gcloud dataproc clusters update "${CLUSTER_NAME}" \
    --region=${REGION} \
    --num-workers="${num_workers}" 
