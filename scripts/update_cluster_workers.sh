#!/bin/sh

num_workers=$1

gcloud dataproc clusters update ${CLUSTER_NAME} \
    --region=europe-west3-a \
    --num-workers="${num_workers}" 
