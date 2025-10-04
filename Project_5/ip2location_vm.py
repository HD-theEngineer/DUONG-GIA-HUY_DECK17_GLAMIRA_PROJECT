from pymongo import MongoClient
import IP2Location
import os
import jsonlines
import time


def init_mongodb(mongodb_port, db_name, collection_name):
    try:
        # Initiate connection to MongoDB
        client = MongoClient(f"mongodb://localhost:{mongodb_port}/")

        # Test connection
        print(f"Connected to MongoDB:\n{client.admin.command('ping')}\n")
        # Choose database and collection
        db = client[db_name]
        collection = db[collection_name]
        # ip2loc = db[ip_collection]

        return collection, client

    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def load_ip2location(path):
    # Load IP2Location database
    try:
        ip_db = IP2Location.IP2Location(str(path))
        print("Load ip2location data successfully.")
        return ip_db
    except FileNotFoundError:
        print(f"Error: IP2Location database not found at {path}")
        return None


def batch_query_mongodb(collection, batch_size):
    last_ip = None
    while True:
        pipeline = []
        # If this is not the first batch, add a $match stage to start from the last processed IP
        if last_ip:
            pipeline.append({"$match": {"ip": {"$gt": last_ip}}})
        # Add these stages to pipeline
        pipeline.append({"$group": {"_id": "$ip"}})
        pipeline.append({"$sort": {"_id": 1}})
        pipeline.append({"$limit": batch_size})
        print(f"Running this pipeline:... \n{pipeline}")

        # Execute the pipeline
        ip_batch_cursor = collection.aggregate(pipeline)
        ip_list = list(ip_batch_cursor)

        # Condition to check if we processed all documents
        if not ip_list:
            print("\nAll unique IPs have been processed.")
            break

        # Update last IP for next batch
        last_ip = ip_list[-1]["_id"]
        print(f"Last IP of this batch: {last_ip}")
        yield ip_list


def convert_ip2location(ip_list, ip_db):
    enriched_data = []
    # Process the batch
    for doc in ip_list:
        ip = doc["_id"]
        ip_data = {"ip": ip}
        try:
            rec = ip_db.get_all(ip)
            ip_data.update(rec.__dict__)
            enriched_data.append(ip_data)
        except Exception as e:
            print(f"Error looking up IP {ip}: {e}")
    return enriched_data


"""
def load_to_mongodb(new_collection, data):
    chunk_data = []
    chunk_size = 1000
    for i in range(0,len(data),chunk_size):
        chunk_data = data[i:i+chunk_size]
        operation = []
        for doc in chunk_data:
            operation.append(
                UpdateOne({'$set': doc}, upsert=True)
            )
        if operation:
            try:
                new_collection.bulk_write(operation, ordered=False)
                print(f"Bulk write {i} documents completed.")
            except Exception as e:
                print(f"Error writing data to mongoDB: {e}")
"""

"""
def insert_to_mongodb(collection, data):
    try:
        collection.insert_many(data, ordered=False)
    except Exception as e:
        print(f"Error during bulk insert: {e}")
"""


def save_to_jsonl(data, data_num, output_dir="ip2location"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, f"location_data_{data_num}.jsonl")
    try:
        with jsonlines.open(file_path, mode="w") as writer:
            for item in data:
                writer.write(item)
        print(f"Saved batch {data_num} to {file_path}")
    except Exception as e:
        print(f"Error saving batch {data_num} to JSON: {e}")


def main():
    # Pagination variables
    mongodb_port = 27017
    db_name = "project-5"
    collection_name = "summary"
    ip2_location_path = "D:\\glamira-data\\IP2LOCATION-IPV6.BIN"
    # ip_collection = "ip2loc"

    # Load ip2location
    ip_db = load_ip2location(ip2_location_path)

    # Connect to mongoDB
    my_collection, client = init_mongodb(mongodb_port, db_name, collection_name)

    # Process IPs by batch
    processed_count = 0
    batch_num = 0

    for batch in batch_query_mongodb(my_collection, batch_size=100000):
        start = time.perf_counter()
        batch_num += 1
        print(f"Processing batch #{batch_num} with {len(batch)} unique IPs...")

        # Proceed to convert IP in list to location
        enriched_ip_data = convert_ip2location(batch, ip_db)
        processed_count += len(enriched_ip_data)

        # Load data back to MongoDB for further processing
        # insert_to_mongodb(ip2loc, enriched_ip_data)

        # Save data into json for later crawling
        save_to_jsonl(enriched_ip_data, batch_num)
        print(f"Processed: {processed_count}")
        end = time.perf_counter()
        print(f"Time taken for batch #{batch_num}: {end - start:.2f} seconds\n")
    # Close the MongoDB connection
    client.close()
    print("Processing complete. MongoDB connection closed.")


if __name__ == "__main__":
    main()
