#!/bin/sh

num_workers=$1
output_timestamp=$(date +"%Y-%m-%d_%H-%M-%S")

# Get current number of workers in the cluster
current_workers=$(gcloud dataproc clusters describe "${CLUSTER_NAME}" \
    --region=europe-west3-a \
    --format="value(config.secondaryWorkerConfig.numInstances,config.workerConfig.numInstances)" | tr '\t' '+' | bc)

if [ -z "$num_workers" ]; then
    echo "Error: Please provide number of workers as first argument"
    exit 1
fi

# Compare with current cluster configuration
if [ "$num_workers" != "$current_workers" ]; then
    echo "Updating cluster to ${num_workers} workers..."
    gcloud dataproc clusters update ${CLUSTER_NAME} \
    --region=europe-west3-a \
    --num-workers="${num_workers}" 
fi

gcloud dataproc jobs submit spark --cluster="${CLUSTER_NAME}" \
    --region=europe-west3-a \
    --jar="gs://${BUCKET_NAME}/scala/co-purchase-analysis_2.12-0.1.0.jar" \
    -- "gs://${BUCKET_NAME}/input/" "gs://${BUCKET_NAME}/output/run_${output_timestamp}/" "${num_workers}"