#!/bin/sh

gcloud dataproc clusters create ${CLUSTER_NAME} --region=europe-west3-a --single-node --master-boot-disk-size=240 --worker-boot-disk-size=240