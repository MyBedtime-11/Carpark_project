import requests
import pandas as pd
import os
import csv
from datetime import datetime, timedelta


def process_carpark_data():
    today = datetime.now()
    date_str = today.strftime("%Y-%m-%d_%H")

    url = "https://api.data.gov.sg/v1/transport/carpark-availability"

    respnse = requests.get(url)
    record = []
    if respnse.status_code == 200:
        data = respnse.json()
        print("Data retrieved successfully.")

        # Define the file path and folder path
        # Separating makes it possible to go through the folder to delete old records
        folderpath = r"C:\Users\Guo Xiang\OneDrive\Pictures\Documents\Analyst Course\Data Pipeline"
        filename = f"carpark_data_{date_str}.csv"
        filepath = os.path.join(folderpath, filename)

        # Get the timestamp for the data collected
        timestamp = data["items"][0]["timestamp"]

        for carpark in data["items"][0]["carpark_data"]:
            # Extract carpark information
            info = carpark["carpark_info"][0]
            carpark_number = carpark["carpark_number"]

            # Append carpark information to the record
            record.append(
                {
                    "carpark_number": carpark_number,
                    "lot_type": info["lot_type"],
                    "lots_available": int(info["lots_available"]),
                    "total_lots": int(info["total_lots"]),
                    "timestamp": timestamp,
                }
            )

        df = pd.DataFrame(record)

        # df = df[df["lot_type"].isin(["C", "H", "Y"])]

        df.to_csv(
            filepath,
            index=False,
            lineterminator="\n",
            quoting=csv.QUOTE_ALL,
            encoding="utf-8",
        )
        print(f"File {filepath} created successfully.")

        # Delete old records
        delete_old_records(folderpath)


def delete_old_records(folder):
    # Get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Loop through files in the folder
    # and delete files that is from the day before
    for filename in os.listdir(folder):
        if filename.startswith(f"carpark_data_{yesterday_str}"):
            filepath = os.path.join(folder, filename)
            os.remove(filepath)
            print(f"Deleted old file: {filepath}")


def main():
    process_carpark_data()


if __name__ == "__main__":
    main()
