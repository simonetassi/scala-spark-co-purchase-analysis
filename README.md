# Co-Purchase Analysis

This repository contains the implementation of the **Co-Purchase Analysis** project for the _Scalable and Cloud Programming_ course at the University of Bologna (Academic Year 2024–2025).

## Project Overview

This project analyzes co-purchase patterns from a dataset of customer orders using Apache Spark. Given a CSV file with order-product pairs, the application identifies products that are frequently bought together.

### Input Format

The expected input is a CSV file with the following structure:


`order_id,product_id`

Each row represents the purchase of a product (`product_id`) within an order (`order_id`).

### Output


The generated output is a CSV file containing product co-purchase relationships with the following format:

`product1_id,product2_id,co_purchase_count`

Where:

- `product1_id` and `product2_id` represent a pair of products that were purchased together
- `co_purchase_count` indicates the number of times this specific product pair appeared in the same order

## Prerequisites

### Local Execution

- **Scala 2.12**
- **Apache Spark 3.x**
- **SBT (Scala Build Tool)**

### Google Cloud Platform (GCP)

- **Google Cloud CLI (gcloud)** installed and configured
- **GCP Project** with billing enabled
- **Required APIs enabled:**
    - Cloud Dataproc API
    - Cloud Storage API
- **IAM Permissions:**
    - Dataproc Admin
    - Storage Admin

## Getting Started

### Local Execution

1. **Build the project:**
    
    ```bash
    cd co-purchase-analysis/
    sbt clean package
    ```
    
2. **Run the analysis:**
    
    ```bash
    spark-submit target/scala-2.12/co-purchase-analysis_2.12-0.1.0.jar \
      inputs/order_products_example.csv \
      output/
    ```
    

### Google Cloud Platform Execution

1. **Configure gcloud CLI:**
    
    ```bash
    gcloud init
    ```
    
2. **Set environment variables:**
    
      ```bash
      # Example configuration
      export CLUSTER_NAME=scp
      export BUCKET_NAME=co-purchase-storage
      export INPUT_PATH=./co-purchase-analysis/inputs/order_products.csv
      export OUTPUT_PATH=./co-purchase-analysis/outputs # ensure directory exists before running
      export REGION=europe-west6
      export ZONE=europe-west6-b
      ```

	 If `$REGION` or `$ZONE` variables are changed, run `scripts/update-location.sh` to update the configuration.

 3. **Execute the complete pipeline:**
    
      ```bash
      scripts/create-bucket.sh; \
      scripts/upload-jar.sh; \
      scripts/submit-job.sh 1; \ 
      scripts/submit-job.sh 2; \
      scripts/submit-job.sh 3; \
      scripts/submit-job.sh 4; \
      scripts/obtain-results.sh; \
      scripts/cleanup.sh;
      ```

    Each `submit-job.sh` call corresponds to a specific phase or variation of the analysis.
## Project Structure

```
co-purchase-analysis/
├── inputs/               # Example input files 
├── src/main/scala/       # Scala Spark job implementation
└── build.sbt             # SBT build configuration
scripts/                  # GCP automation scripts
README.md 
```

---
**Author:** Simone Tassi    
**Course:**  University of Bologna – Scalable and Cloud Programming (2024–2025)