#!/bin/sh

gcloud storage buckets create gs://${BUCKET_NAME} --location=eu
gcloud storage cp ${INPUT_PATH} gs://${BUCKET_NAME}/input/