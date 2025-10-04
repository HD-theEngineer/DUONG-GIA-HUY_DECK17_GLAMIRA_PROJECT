import IP2Location
import os
import json


def main():
    path = "D:\glamira-data\IP2LOCATION-LITE-DB9.BIN"

    try:
        ip_db = IP2Location.IP2Location(path)
        print("Load ip2location data successfully.")
    except FileNotFoundError:
        print(f"Error: IP2Location database not found at {path}")

    test_ip = "1.121.148.38"
    location_data = {"ip": test_ip}

    try:
        rec = ip_db.get_all(test_ip)
        # print(type(rec))
        # converted_data = {"ip": ip, "country": rec.country_long, "city": rec.city}
        location_data.update(rec.__dict__)
        print(location_data)
    except Exception as e:
        print(f"Error looking up IP {test_ip}: {e}")


if __name__ == "__main__":
    main()
