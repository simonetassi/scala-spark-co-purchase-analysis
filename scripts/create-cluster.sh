#!/bin/sh

num_workers=${1:-1}  # Default to 1 if no argument provided

# Validate num_workers parameter
if [ "$num_workers" -lt 1 ] || [ "$num_workers" -gt 4 ]; then
   echo "Error: Number of workers must be between 1 and 4"
   echo "Usage: $0 [num_workers]"
   exit 1
fi

# Create cluster based on number of workers
if [ "$num_workers" -eq 1 ]; then
   echo "Creating single-node cluster..."
   gcloud dataproc clusters create "${CLUSTER_NAME}" \
       --region="${REGION}" \
       --single-node \
       --master-boot-disk-size=400 \
       --master-machine-type=n2-standard-4
else
   echo "Creating multi-node cluster with $num_workers worker nodes..."
   gcloud dataproc clusters create "${CLUSTER_NAME}" \
       --region="${REGION}" \
       --num-workers="$num_workers" \
       --master-boot-disk-size=400 \
       --worker-boot-disk-size=400 \
       --master-machine-type=n2-standard-4 \
       --worker-machine-type=n2-standard-2
fi