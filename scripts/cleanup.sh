gcloud dataproc clusters delete "${CLUSTER_NAME}" \
    --region=${REGION}
gcloud storage rm gs:// --recursive