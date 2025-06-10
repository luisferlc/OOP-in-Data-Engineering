# Goal of the project
Utilize OOP (Object-Oriented Programming) with Python to create a Data Engineering project that grabs data from a PostgreSQL database, save CSV files locally and then upload those files to AWS S3.

Why to do this?

I have found that there is very little content out there regarding the use of OOP into Data Engineering. If the discussion was about the use of OOP in Software Engineering or Web Devlopment, there are tons of mountains of content to use from. I was eager to find some books on this but no luck either. Most of the cases I have found that the gold standard principles of Software Engineering Development can be adapted to Data Engineering so, this is an attemp to do that and invite Data Engineers to include OOP in their projects too.

### Advantages of OOP in DE
1. Modularity and Reusability
You can create reusable classes like DataIngestor, DataTransformer, or DataValidator. Once you write them, you can easily plug them into different pipelines or projects with minimal changes.

2. Scalability
OOP makes it easier to scale your architecture. Need to support a new data source or destination? Just extend a base class with a new child class (inheritance) and override only the parts you need to tweak.

3. Clean Code and Maintenance
Encapsulation keeps logic tidy and separated. When your transformation logic is tucked inside a method like .clean_dates() or .normalize_columns(), it's easier to debug and test individual parts without breaking the whole system.

4. Testing and CI/CD Friendly
With well-structured classes and methods, you can write targeted unit tests for each step in your pipeline. That’s gold for CI/CD setups and avoiding regressions.

5. Intuition and Collaboration
For larger teams or when working across squads, OOP makes your code self-documenting. Anyone reading your class structure can follow the data flow like a story: ingest, clean, transform, and load.

### Structure of this project
libraries/ -> this folder contains the utils.py file that has the classes and functions to make the pipeline work.
- class PostgreSQLConnector: when initiated, it creates a connection to the PostgreSQL based on a .config file that contains the information required. There are two private methods that do this, and these are private because they do not need to be used by the general users.
Three public methods:
  - test_connection(): user way to confirm that the connection to the database is correct.
  - gen_full_load_csv(): generate a CSV that contains the full data for a given table passed as a parameter.
  - gen_incremental_csv(): generate a CSV that contains the incremental data based on last timestamp of upload to S3, for a given table passed as a parameter.
- class S3Actionable: when initiated it contains the required AWS keys to be able to upload data to S3. One private method that reads the secret keys from the .config file.
Two public methods:
  - upload_file(): grab the local CSV file and give a name to upload it to a specific S3 Bucket all passed as parameters.
  - get_last_update_from_s3(): get the last timestamp of upload to S3 bucket. As of now, the bucket is hardcoded in the initiation of the class.
- generate_timestamp_json() function independent: use to generate the timestamp of upload to S3 for the use of the gen_incremental_csv() method inside PostgreSQLConnector class.


