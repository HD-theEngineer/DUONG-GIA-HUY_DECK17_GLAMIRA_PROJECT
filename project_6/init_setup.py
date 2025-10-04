import jsonlines
import logging
import os
from pymongo import MongoClient
import time
from google.cloud import storage

config_file_path = "config.jsonl"
##-DEFINED FUNCTIONS-------------------------------------------------------##


def setup_logging():
    logging.basicConfig(
        filename="logs/pipeline.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def import_config(config_file_path=config_file_path):
    if not os.path.exists(config_file_path):
        print(f"Error: Cannot find '{config_file_path}'. Please check again.")
        return None
    configs = {}
    try:
        with jsonlines.open(config_file_path, mode="r") as reader:
            for config in reader:
                for key, value in config.items():
                    configs[key] = value
    except jsonlines.Error as e:
        print(f"Error during operation of '{config_file_path}':\n{e}")
        return None

    if not configs:
        print("No valid values in the file.")
    else:
        # gcp_config = configs.get("gcp_config")
        mongodb_config = configs.get("mongodb_config")
        export_config = configs.get("export_config")
        gcs_config = configs.get("gcs_config")

    return mongodb_config, export_config, gcs_config


def mongodb_connect(mongodb_config):
    local_port = mongodb_config.get("local_port")
    db_name = mongodb_config.get("db_name")
    collection_name = mongodb_config.get("collection_name")
    try:
        start = time.perf_counter()
        client = MongoClient(f"mongodb://localhost:{local_port}/")
        db = client[db_name]
        collection = db[collection_name]
        print(f"Connected to MongoDB:\n{client.admin.command('ping')}\n")
        end = time.perf_counter()
        print(f"Connection established in {end - start} seconds.")
        return client, collection
    except Exception as e:
        print(f"Error connecting to Mongodb:\n{e}")
        return None


def connect_gcs(gcs_config, file_dir):
    bucket_name = gcs_config.get("bucket_name")
    project_id = gcs_config.get("project_id")

    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        return bucket
    except Exception as e:
        print(f"Error uploading to GCS:\n{e}")


"""
def create_ssh_tunnel(gcp_config, mongodb_config):
    vm_name = gcp_config["vm_name"]
    project_id = gcp_config["project_id"]
    zone = gcp_config["zone"]
    local_port = mongodb_config["local_port"]
    vm_port = mongodb_config["vm_port"]

    ssh_command = [
        "gcloud",
        "compute",
        "ssh",
        vm_name,
        f"--project={project_id}",
        f"--zone={zone}",
        "--",
        "-L",
        f"{local_port}:127.0.0.1:{vm_port}",
        "-N",
    ]
    try:
        print("SSH Tunneling via gcloud...")
        # Let ssh command run in background
        process = subprocess.Popen(ssh_command)
        # wait for tunnel to establish
        time.sleep(3)
        print("SSH tunnel established.")
        return process
    except Exception as e:
        print(f"Error during ssh tunnel:\n{e}")
        return None
"""

##-MAIN FUNCTIONS-----------------------------------------------------------##


def main():
    pass


if __name__ == "__main__":
    main()
