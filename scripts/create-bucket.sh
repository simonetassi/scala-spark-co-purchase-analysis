#!/bin/sh

gcloud storage buckets create gs://${BUCKET_NAME} --location=eu
echo "${BUCKET_NAME} bucket created."

gcloud storage cp "${INPUT_PATH}" gs://${BUCKET_NAME}/input/
echo "Input file copied to ${BUCKET_NAME} bucket."