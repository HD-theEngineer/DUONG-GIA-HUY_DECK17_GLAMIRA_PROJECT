#!/bin/bash

gcloud functions deploy gcs_bq_trigger \
    --runtime python312 \
    --trigger-resource glamira-dgh \
    --trigger-event google.storage.object.finalize \
    --entry-point gcs_to_bigquery \
    --region asia-southeast1 \
    --memory 512MB \
    --set-env-vars GCP_PROJECT=angular-vortex-466913-g6,DATASET_NAME=glamira_data,TABLE_NAME=test
    