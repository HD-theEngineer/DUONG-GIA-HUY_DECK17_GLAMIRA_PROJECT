from init_setup import import_config, mongodb_connect, connect_gcs
import jsonlines
import os
import time

##-DEFINED FUNCTIONS-------------------------------------------------------##


def main_pipeline(mongodb_config, gcs_config, export_config):
    client, collection = mongodb_connect(mongodb_config)
    bucket = connect_gcs(gcs_config, file_dir=export_config.get("monitor_dir"))

    if client and bucket:
        try:
            file_name = export_config.get("file_name")
            file_dir = export_config.get("monitor_dir")
            batch_size = export_config.get("batch_size")
            last_id = None
            batch_num = 1
            query = {}

            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            while True:
                try:
                    start = time.perf_counter()

                    # 1 Query data in batch from MongoDB
                    if last_id:
                        query = {"_id": {"$gt": last_id}}
                    cursor = collection.find(query).sort("_id", 1).limit(batch_size)
                    batch = list(cursor)
                    last_id = batch[-1]["_id"]

                    # 2 Data transformation before write to JSONL
                    for item in batch:
                        if "cart_products" in item and isinstance(
                            item["cart_products"], list
                        ):
                            for product in item["cart_products"]:
                                if "option" in product and product["option"] == "":
                                    product["option"] = []
                        item["_id"] = str(item["_id"])
                    if not batch:
                        print("Finished exporting all documents from mongoDB.")
                        break

                    # 2.2 Save to JSONL file
                    file_path = os.path.join(file_dir, f"{file_name}_{batch_num}.jsonl")
                    with jsonlines.open(file_path, mode="w") as writer:
                        for item in batch:
                            writer.write(item)
                    print(f"Exported {batch_num * batch_size} docs to {file_path}.")

                    # 3 Upload to GCS
                    print(f"Uploading {os.path.basename(file_name)} to GCS...")
                    blob = bucket.blob(f"exported/{file_name}_{batch_num}.jsonl")
                    blob.upload_from_filename(file_path)
                    print(f"Uploaded {file_name} to {bucket}.")

                    batch_num += 1
                    end = time.perf_counter()
                    print(f"Processing time for this batch is {end - start} seconds.")

                    # break
                except Exception as e:
                    print(f"Error during query from MongoDB: {e}")
        except Exception as e:
            print(f"Error during processing:\n{e}")
        finally:
            client.close()
            print("MongoDB connection closed.")


##-MAIN FUNCTION-----------------------------------------------------------##
def main():
    # setup_logging()
    mongodb_config, export_config, gcs_config = import_config()
    if mongodb_config and gcs_config and export_config:
        try:
            main_pipeline(mongodb_config, gcs_config, export_config)
        except Exception as e:
            print(f"Error in main pipeline:\n{e}")


##-RUN SCRIPT CONDITION----------------------------------------------------##

if __name__ == "__main__":
    main()
