#!/bin/bash

# Download the PageRank data
curl -o small_page_links.nt https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt

# Configurations
PROJECT_ID="pagerank-441714"
REGION="europe-west1"
ZONE="europe-west1-c"
BUCKET_NAME="pagerank"
DATA_PATH="gs:///public_lddm_data/"
PYSPARK_SCRIPT="gs://$BUCKET_NAME/dataproc.py"
OUTPUT_DIR="gs://$BUCKET_NAME/out"

# Fonction pour créer un cluster
create_cluster() {
  local cluster_name=$1
  local num_workers=$2
  echo "Creating cluster: $cluster_name with $num_workers workers..."
  gcloud dataproc clusters create $cluster_name \
    --enable-component-gateway \
    --region $REGION \
    --zone $ZONE \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 50 \
    --num-workers $num_workers \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 50 \
    --image-version 2.0-debian10 \
    --project $PROJECT_ID
}

# Fonction pour exécuter un job PySpark
run_job() {
  local cluster_name=$1
  local implementation=$2
  local partitioning=$3
  echo "Running job on cluster: $cluster_name with implementation: $implementation and partitioning: $partitioning..."
  
  # Lancer le job et capturer l'ID du job
  job_id=$(gcloud dataproc jobs submit pyspark $PYSPARK_SCRIPT \
    --cluster $cluster_name \
    --region $REGION \
    --format="value(reference.jobId)" \
    -- \
    --implementation $implementation \
    --partitioning $partitioning \
    --data-path $DATA_PATH \
    --output-path $OUTPUT_DIR/$implementation/$partitioning/)

  # Attendre la fin du job
  echo "Waiting for job $job_id to complete..."
  gcloud dataproc jobs wait $job_id --region $REGION
}

# Fonction pour supprimer un cluster
delete_cluster() {
  local cluster_name=$1
  echo "Deleting cluster: $cluster_name..."
  gcloud dataproc clusters delete $cluster_name --region $REGION --quiet
}

# Nettoyage du dossier sortie
echo "Cleaning output directory..."
gsutil -m rm -rf $OUTPUT_DIR

# Lancer les configurations de tests
for num_nodes in 0 1 2; do  # 0 workers = 1 noeud total, etc.
  cluster_name="pagerank-cluster-$num_nodes"
  create_cluster $cluster_name $num_nodes
  
  for implementation in "DataFrame" "RDD"; do
    for partitioning in "enabled" "disabled"; do
      run_job $cluster_name $implementation $partitioning
    done
  done
  
  delete_cluster $cluster_name
done

echo "All jobs completed!"
