import configparser
import pathlib
import psycopg2
from psycopg2 import sql
import csv
import sys

"""The script is designed to connect to a Redshift database, retrieve data from a specified table, and save it to a CSV file.
It first reads configuration variables (e.g., username, password, host, etc.) from a configuration file.
The connect_to_redshift function establishes a connection to the Redshift instance using the provided credentials.
The download_redshift_data function constructs and executes a SQL query to select data from a specified table, fetches the result, and writes it to a CSV file.
In the script's main section, you specify the table name and output file path, and then call the functions to perform the data extraction."""


# Parse configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

# Store configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
DATABASE = parser.get("aws_config", "redshift_database")

def connect_to_redshift():
    """Connect to a Redshift instance."""
    try:
        # Establish a connection to the Redshift cluster
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
        return rs_conn
    except Exception as e:
        print(f"Unable to connect to Redshift. Error: {e}")
        sys.exit(1)

def download_redshift_data(rs_conn, table_name, output_file):
    """Download data from a Redshift table to a CSV file."""
    with rs_conn:
        with rs_conn.cursor() as cur:
            # Build a SQL query to select all data from the specified table
            query = sql.SQL("SELECT * FROM {table};").format(table=sql.Identifier(table_name))
            cur.execute(query)
            # Fetch the query result, including column headers
            result = cur.fetchall()
            headers = [col[0] for col in cur.description]
            result.insert(0, tuple(headers))
            # Write the data to a CSV file
            with open(output_file, "w", newline="") as fp:
                myFile = csv.writer(fp)
                myFile.writerows(result)

if __name__ == "__main__":
    table_name = "reddit"  # Specify the name of the Redshift table to download
    output_file = "/tmp/redshift_output.csv"  # Specify the output file path
    rs_conn = connect_to_redshift()
    download_redshift_data(rs_conn, table_name, output_file)

