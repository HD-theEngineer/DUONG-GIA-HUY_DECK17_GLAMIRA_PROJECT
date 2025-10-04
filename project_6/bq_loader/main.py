from google.cloud import bigquery
import json
import os

client = bigquery.Client()

project_id = os.getenv("GCP_PROJECT")
dataset = os.getenv("DATASET_NAME", "glamira_data")
table = os.getenv("TABLE_NAME", "temp_raw_data")
# temp_table = f"{table}_temp"

table_fqn = f"{project_id}.{dataset}.{table}"
# temp_table_fqn = f"{project_id}.{dataset}.{temp_table}"

# unique_key = "_id"


def gcs_to_bigquery(event, context):
    try:
        bucket = event.get("bucket")
        name = event.get("name")
        print(f"Triggered: gs://{bucket}/{name}")

        if not bucket or not name:
            print("Missing bucket/name; nothing to do.")
            return

        if not name.startswith("exported/"):
            print("Not in exported/ prefix — skipping.")
            return

        try:
            with open("raw_schema.json", "r") as f:
                schema_def = json.load(f)
            schema = [bigquery.SchemaField.from_api_repr(s) for s in schema_def]
        except Exception as e:
            print("Failed to load schema:", e)
            raise

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        uri = f"gs://{bucket}/{name}"
        print(f"Starting load job {uri} -> {table_fqn}")
        load_job = client.load_table_from_uri(uri, table_fqn, job_config=job_config)
        load_job.result()
        print("Load to temp completed.")

        # # Build and run MERGE (UPSERT)
        # cols = [f.name for f in schema]
        # cols_str = ", ".join(cols)
        # src_cols = ", ".join([f"TT.{c}" for c in cols])
        # update_set = ", ".join([f"{c}=TT.{c}" for c in cols if c != unique_key])

        # merge_sql = f"""
        # MERGE `{table_fqn}` ET
        # USING `{temp_table_fqn}` TT
        # ON ET.{unique_key} = TT.{unique_key}
        # WHEN MATCHED THEN
        #   UPDATE SET {update_set}
        # WHEN NOT MATCHED THEN
        #   INSERT ({cols_str}) VALUES ({src_cols})
        # """
        # query_job = client.query(merge_sql)
        # query_job.result()
        # print("Merge completed.")

    except Exception as e:
        print(f"Error processing: {e}")
        raise
