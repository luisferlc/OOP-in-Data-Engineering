import boto3
from botocore.exceptions import BotoCoreError, ClientError
import configparser
import psycopg2
from psycopg2 import OperationalError, Error
import json
from datetime import datetime
import csv
import logging


class PostgreSQLConnector:
    def __init__(self, config_file="D:/Python Projects/Data Pipelines/config/pipeline.conf"):
        self.config_file = config_file
        self.hostname, self.port, self.username, self.password, self.dbname = self._generate_db_parser()

    def _generate_db_parser(self):
        """Reads PostgreSQL credentials from a config file."""
        parser = configparser.ConfigParser()
        parser.read(self.config_file)

        hostname = parser.get("postgresql_config", "hostname")
        port = parser.get("postgresql_config", "port")
        username = parser.get("postgresql_config", "username")
        password = parser.get("postgresql_config", "password")
        dbname = parser.get("postgresql_config", "database")

        return hostname, port, username, password, dbname

    def _connect_to_db(self):
        """Creates and returns a new PostgreSQL connection."""
        try:
            return psycopg2.connect(
                dbname=self.dbname,
                user=self.username,
                password=self.password,
                host=self.hostname,
                port=self.port
            )
        except OperationalError as e:
            print(f"Database connection error: {e}")
            return None

    def test_connection(self):
        """Tests the database connection."""
        connection = self._connect_to_db()
        if connection:
            print("Connection to PostgreSQL database successful!")
            connection.close()

    def gen_full_load_csv(self, table_name, local_filename):
        """Fetches all rows from the specified table and saves them as a CSV file."""
        connection = self._connect_to_db()
        if connection:
            try:
                with connection.cursor() as cursor:
                    query = f"SELECT * FROM {table_name};"
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    if rows:
                        # Save results to a CSV file
                        with open(local_filename, "w", newline="") as fp:
                            csv_w = csv.writer(fp, delimiter=",")
                            csv_w.writerows(rows)

                        print(f"File '{local_filename}' generated successfully.")
                    else:
                        print("There is no data inside the table provided.")
            except Error as e:
                print(f"Query execution error: {e}")
            finally:
                connection.close()

    def gen_incremental_csv(self, table_name, last_update_time,timestamp_column:str,local_filename):
        """Fetches rows from the table that were updated after the last recorded timestamp in the S3 JSON."""
        if last_update_time is None:
            print("No previous timestamp found in S3.")
            return []

        # Normalize JSON timestamp
        last_update_dt = datetime.strptime(last_update_time, "%Y-%m-%dT%H:%M:%S.%f")

        connection = self._connect_to_db()
        if connection:
            try:
                with connection.cursor() as cursor:
                    query_incremental = f"SELECT * FROM {table_name} WHERE {timestamp_column} > %s;"
                    cursor.execute(query_incremental, (last_update_dt,))
                    rows = cursor.fetchall()

                    if rows:
                        # Save results to a CSV file
                        with open(local_filename, "w", newline="") as fp:
                            csv_w = csv.writer(fp, delimiter=",")
                            csv_w.writerows(rows)

                        print(f"File '{local_filename}' generated successfully.")
                    else:
                        print("There is no data inside the table provided.")
            except Error as e:
                print(f"Query execution error: {e}")
            finally:
                connection.close()

class S3Actionable:
    def __init__(self, config_file="D:/Python Projects/Data Pipelines/config/pipeline.conf"):
        self.config_file = config_file
        self.access_key, self.secret_key, self.s3 = self._get_aws_credentials()
        self.bucket_timestamp_name = "insertion-times"  # Hardcoded bucket name
        self.key_timestamp = "last_timestamp_ingestion.json"  # Hardcoded filename

    def _get_aws_credentials(self):
        """Reads AWS credentials from a config file and initializes an S3 client."""
        parser = configparser.ConfigParser()
        parser.read(self.config_file)

        access_key = parser.get("aws_boto_credentials", "access_key")
        secret_key = parser.get("aws_boto_credentials", "secret_key")

        s3 = boto3.client("s3",
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

        return access_key, secret_key, s3

    def upload_file(self, local_filename, s3_file, bucket_name):
        """Uploads a file to the configured S3 bucket."""
        try:
            self.s3.upload_file(local_filename, bucket_name, s3_file)
            print("File uploaded successfully!")
        except (BotoCoreError, ClientError) as e:
            print(f"Error uploading file: {e}")

    def get_last_update_from_s3(self):
        """Fetches the last update timestamp from a predefined JSON file in S3."""
        try:
            response = self.s3.get_object(Bucket=self.bucket_timestamp_name, Key=self.key_timestamp)
            json_data = json.loads(response["Body"].read().decode("utf-8"))
            return json_data.get("timestamp") #column insid the json
        except Exception as e:
            logging.error(f"Error fetching JSON from S3: {e}")
            return None

def generate_timestamp_json():
    """Generates a JSON with the current timestamp and saves it locally with today's date as the filename."""
    timestamp_data = {
        "timestamp": datetime.utcnow().isoformat()
    }

    # Generate filename based on today's date
    filename = "D:/Python Projects/Data Pipelines/data/timestamp_json/timestamp_"+f"{datetime.today().strftime('%Y-%m-%d')}.json"

    # Save to a local file
    with open(filename, "w") as file:
        json.dump(timestamp_data, file, indent=4)

    print(f"JSON saved successfully as {filename}")

