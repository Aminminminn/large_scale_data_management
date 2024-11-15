#!/bin/bash

# Configurable variables
PROJECT_ID= $1
BUCKET_NAME="bucket_pyspark"
CLUSTER_NAME="pyspark-cluster"
REGION="europe-west1"
ZONE="europe-west1-c"
INPUT_FILE_NAME="small_page_links.nt"
OUTPUT_PATH="output_rdd"
PAGERANK_SCRIPT="pagerank_RDD.py"
ITERATIONS=10  # Number of iterations for PageRank

# Download the PageRank data
curl -o $INPUT_FILE_NAME https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt

# Enable required APIs
gcloud services enable dataproc.googleapis.com storage.googleapis.com

# Set the default project
gcloud config set project $PROJECT_ID

# Create the GCS bucket if it does not exist
gsutil mb -l $REGION gs://$BUCKET_NAME/

# Upload the input file to the bucket
gsutil cp $INPUT_FILE_NAME gs://$BUCKET_NAME/

# Create the PySpark PageRank script using RDDs
cat << EOF > $PAGERANK_SCRIPT
import re
import sys
import time
from operator import add
from typing import Iterable, Tuple

from pyspark.resultiterable import ResultIterable
from pyspark.sql import SparkSession

def computeContribs(urls: Iterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls: str) -> Tuple[str, str]:
    """Parses a URL pair string into URLs pair."""
    parts = re.split(r'\\s+', urls)
    return parts[0], parts[2]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Start the overall timer for the script
    start_time = time.time()

    # Start the Spark session
    spark_start_time = time.time()
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()
    spark_init_time = time.time() - spark_start_time  # Time taken to initialize Spark

    # Read input parameters
    input_path = sys.argv[1]
    iterations = int(sys.argv[2])

    # Start loading data timer
    data_load_start_time = time.time()
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    data_load_time = time.time() - data_load_start_time  # Time taken to load data

    # Start PageRank computation timer
    pagerank_start_time = time.time()
    for iteration in range(iterations):
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    pagerank_time = time.time() - pagerank_start_time  # Time taken to compute PageRank

    # Start saving results timer
    save_start_time = time.time()
    output_path = "gs://$BUCKET_NAME/$OUTPUT_PATH"
    ranks.saveAsTextFile(output_path)
    save_time = time.time() - save_start_time  # Time taken to save results

    # Total execution time
    execution_time = time.time() - start_time

    # Write the execution times to a local file
    execution_file_path = "/tmp/execution_time.txt"
    with open(execution_file_path, "w") as f:
        f.write(f"Spark initialization time (seconds): {spark_init_time:.2f}\n")
        f.write(f"Data loading time (seconds): {data_load_time:.2f}\n")
        f.write(f"PageRank computation time (seconds): {pagerank_time:.2f}\n")
        f.write(f"Result saving time (seconds): {save_time:.2f}\n")
        f.write(f"Total execution time (seconds): {execution_time:.2f}\n")

    # Upload the execution time file to GCS
    time_output_path = f"{output_path}/execution_time.txt"
    spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    ).copyFromLocalFile(False, True, execution_file_path, time_output_path)

    # Stop the Spark session
    spark.stop()
EOF

# Create a minimal Dataproc cluster (single-node)
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --image-version=2.0-debian10

# Submit the PySpark job to the cluster
gcloud dataproc jobs submit pyspark $PAGERANK_SCRIPT \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    -- gs://$BUCKET_NAME/$INPUT_FILE_NAME $ITERATIONS

# List the results in GCS
gsutil ls gs://$BUCKET_NAME/$OUTPUT_PATH/

# Delete the cluster after execution
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
