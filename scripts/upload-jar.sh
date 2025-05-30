#!/bin/sh

cd ./co-purchase-analysis
sbt clean package
cd ..

gcloud storage cp co-purchase-analysis/target/scala-2.12/co-purchase-analysis_2.12-0.1.0.jar gs://${BUCKET_NAME}/scala/co-purchase-analysis_2.12-0.1.0.jar
