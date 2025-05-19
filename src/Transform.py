import pymysql
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

table_name = "carpark_lot_history"


# Delete records older than 30 days
def delete_old_records(conn):
    yesterday = datetime.now() - timedelta(days=30)
    date_str = yesterday.strftime("%Y-%m-%d")

    delete_query = (
        f"DELETE FROM {table_name} WHERE timestamp LIKE %s"  # use parameter placeholder
    )

    with conn.cursor() as cursor:
        cursor.execute(delete_query, (date_str + "%",))  # pass value as tuple
    conn.commit()


# Create table if it doesn't exist
def create_new_table(conn):
    # Check if the table already exists
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()

        if result:
            print("Table already exists.")
            return

        # Create the new table
        create_query = f"""
        CREATE TABLE `{table_name}` (
          `carpark_number` VARCHAR(255),
          `lot_type` VARCHAR(255),
          `lots_available` INT,
          `total_lots` INT,
          `timestamp` DATETIME,
          `day_of_week` VARCHAR(10),
          `hour_of_day` INT
        );
        """
        cursor.execute(create_query)
        print(f"Table `{table_name}` created.")
        conn.commit()


## Transform and load data into the new table
def transform_carpark(conn):
    cursor = conn.cursor()
    transform_query = """
    INSERT INTO carpark_lot_history
    SELECT carpark_number,
    lot_type,
    lots_available,
    total_lots,
    timestamp_cleaned,
    DAYNAME(timestamp_cleaned),
    HOUR(timestamp_cleaned)
    FROM (
        SELECT *, STR_TO_DATE(SUBSTRING_INDEX(timestamp, '+', 1), '%Y-%m-%dT%H:%i:%s') AS `timestamp_cleaned`
        FROM carpark_staging) AS table_time_cleaned;
    """

    cursor.execute(transform_query)
    conn.commit()
    cursor.close()


def main():
    conn = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        port=3306,
        local_infile=True,
        autocommit=True,
    )

    create_new_table(conn)
    delete_old_records(conn)
    transform_carpark(conn)
    print("Data loaded successfully")


if __name__ == "__main__":
    main()
