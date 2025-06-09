#!/bin/sh

num_workers=$1
output_timestamp=$(date +"%Y-%m-%d_%H-%M-%S")

if [ -z "$num_workers" ]; then
   echo "Error: Please provide number of workers as first argument"
   exit 1
fi

# Validate num_workers parameter
if [ "$num_workers" -lt 1 ] || [ "$num_workers" -gt 4 ]; then
   echo "Error: Number of workers must be between 1 and 4"
   exit 1
fi

# Get current cluster configuration using separate gcloud calls for reliability
worker_instances=$(gcloud dataproc clusters describe "${CLUSTER_NAME}" \
   --region="${REGION}" \
   --format="value(config.workerConfig.numInstances)" 2>/dev/null)

master_instances=$(gcloud dataproc clusters describe "${CLUSTER_NAME}" \
   --region="${REGION}" \
   --format="value(config.masterConfig.numInstances)" 2>/dev/null)

if [ $? -eq 0 ]; then
   # Cluster exists - validate and set defaults for empty values
   
   # Handle case where workerConfig doesn't exist (single-node cluster)
   if [ -z "$worker_instances" ] || [ "$worker_instances" = "" ]; then
       worker_instances=0
   fi
   
   # Handle case where masterConfig doesn't exist (shouldn't happen, but be safe)
   if [ -z "$master_instances" ] || [ "$master_instances" = "" ]; then
       master_instances=1
   fi
   
   # Ensure we have valid numbers
   if ! [ "$worker_instances" -eq "$worker_instances" ] 2>/dev/null; then
       worker_instances=0
   fi
   if ! [ "$master_instances" -eq "$master_instances" ] 2>/dev/null; then
       master_instances=1
   fi
   
   # Determine current cluster type
   if [ "$master_instances" -eq 1 ] && [ "$worker_instances" -eq 0 ]; then
       current_workers=1  # Single-node cluster
       is_single_node=true
   else
       current_workers="$worker_instances"
       is_single_node=false
   fi
   
   echo "Current cluster configuration: $current_workers workers (single-node: $is_single_node)"
   echo "Requested configuration: $num_workers workers"
   echo "Debug: master_instances=$master_instances, worker_instances=$worker_instances"
   
   # Check if we need to recreate the cluster
   need_recreate=false
   need_update=false
   
   if [ "$num_workers" -eq 1 ] && [ "$is_single_node" = "false" ]; then
       echo "Need to recreate: switching from multi-node to single-node"
       need_recreate=true
   elif [ "$num_workers" -ne 1 ] && [ "$is_single_node" = "true" ]; then
       echo "Need to recreate: switching from single-node to multi-node"
       need_recreate=true
   elif [ "$num_workers" != "$current_workers" ] && [ "$is_single_node" = "false" ]; then
       echo "Can update cluster workers from $current_workers to $num_workers..."
       need_update=true
   elif [ "$num_workers" = "$current_workers" ]; then
       echo "Cluster already has the correct configuration"
   fi
   
   # Recreate cluster if needed
   if [ "$need_recreate" = "true" ]; then
       echo "Deleting existing cluster..."
       gcloud dataproc clusters delete "${CLUSTER_NAME}" \
           --region="${REGION}" \
           --quiet
       
       echo "Creating new cluster with $num_workers workers..."
       scripts/create-cluster.sh "$num_workers"
   elif [ "$need_update" = "true" ]; then
       echo "Updating cluster workers from $current_workers to $num_workers..."
       gcloud dataproc clusters update "${CLUSTER_NAME}" \
           --region="${REGION}" \
           --num-workers="$num_workers"
   fi
   
else
   # Cluster doesn't exist - create it
   echo "Cluster does not exist. Creating new cluster with $num_workers workers..."
   scripts/create-cluster.sh "$num_workers"
fi

echo "Submitting Spark job..."
gcloud dataproc jobs submit spark --cluster="${CLUSTER_NAME}" \
   --region="${REGION}" \
   --jar="gs://${BUCKET_NAME}/scala/co-purchase-analysis_2.12-0.1.0.jar" \
   -- "gs://${BUCKET_NAME}/input/" "gs://${BUCKET_NAME}/output/run_${num_workers}_workers_${output_timestamp}/"