from libraries.utils import PostgreSQLConnector, S3Actionable, generate_timestamp_json
from datetime import datetime

### Get last_update_time
s3_gen = S3Actionable()
last_update_time = s3_gen.get_last_update_from_s3()
print(last_update_time)

####### DB_Connector object
db_connector = PostgreSQLConnector()

##### Generate names of full load and timestamps
today_date = datetime.today().strftime("%Y-%m-%d")

local_filename = "D:/Python Projects/Data Pipelines/data/source_gened_files/orders_extract_inc_"+today_date+".csv"
s3_filename = "orders_extract_inc_s3_"+today_date+".csv"

json_filename = "D:/Python Projects/Data Pipelines/data/timestamp_json/timestamp_"+f"{datetime.today().strftime('%Y-%m-%d')}.json"
s3_timestamp_filename = "last_timestamp_ingestion.json"

##### Generate CSV with all data from table
db_connector.gen_incremental_csv("orders", last_update_time, "lastupdated",
                                 local_filename)
# Generate JSON Timestamp
generate_timestamp_json()

# Upload to S3
s3_gen.upload_file(local_filename, s3_filename, "lflc-raw-layer")
s3_gen.upload_file(json_filename, s3_timestamp_filename, "insertion-times")
