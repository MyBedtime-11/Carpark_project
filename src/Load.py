import pymysql
import os
from dotenv import load_dotenv
import csv
from datetime import datetime, timedelta

load_dotenv()
table_name = "carpark_staging"


def delete_old_records(conn):
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    delete_query = (
        f"DELETE FROM {table_name} WHERE timestamp LIKE %s"  # use parameter placeholder
    )

    with conn.cursor() as cursor:
        cursor.execute(delete_query, (date_str + "%",))  # pass value as tuple
    conn.commit()


def create_table_if_not_exists(conn, csv_file_path):
    # Check if the table already exists
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()

        if result:
            print("Table already exists.")
            return

        # Read CSV header
        with open(csv_file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)

        # Build CREATE TABLE SQL
        columns = ",\n  ".join([f"`{col.strip()}` TEXT" for col in headers])

        create_query = f"""
        CREATE TABLE `{table_name}` (
          {columns}
        );
        """

        cursor.execute(create_query)
        print(f"Table `{table_name}` created.")
        conn.commit()


def load_csv_to_mysql(conn, csv_file_path):
    with conn.cursor() as cursor:
        load_query = f"""
        LOAD DATA LOCAL INFILE '{csv_file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
        """
        cursor.execute(load_query)
    conn.commit()


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

    today = datetime.today()
    date_str = today.strftime("%Y-%m-%d_%H")

    # folderpath = r"C:\Users\Guo Xiang\OneDrive\Pictures\Documents\Analyst Course\Data Pipeline"
    # filename = f"carpark_data_{date_str}.csv"
    file_path = rf"C:\\Users\\Guo Xiang\\OneDrive\\Pictures\\Documents\\Analyst Course\\Data Pipeline\\carpark_data_{date_str}.csv"

    try:
        create_table_if_not_exists(conn, file_path)
        delete_old_records(conn)
        load_csv_to_mysql(conn, file_path)
        print(f"Data from {file_path} loaded into MySQL successfully.")
    except Exception as e:
        print(f"An error occured: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
