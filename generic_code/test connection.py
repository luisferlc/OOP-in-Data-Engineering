from packages.utils import PostgreSQLConnector

# Instance of the class, automatically calls parser function
db_connector = PostgreSQLConnector()

# Call the function to test the connection
db_connector.test_connection()
