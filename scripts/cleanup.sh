gcloud dataproc clusters delete ${CLUSTER_NAME} \
    --region=europe-west3-a
gcloud storage rm gs://${BUCKET_NAME}/ --recursive