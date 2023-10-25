import configparser
import pathlib
import psycopg2
import sys
from validation import validate_input  # Assuming this is an external validation function
from psycopg2 import sql

"""This script is designed to upload data from an S3 bucket to a Redshift table.
It reads configuration variables from a configuration file and uses command-line arguments for specifying the output file.
The main function orchestrates the entire process, connecting to Redshift, creating a temporary table, copying data from S3 to Redshift, performing data transformations, and committing the transaction.
The script includes functions for connecting to Redshift, loading data, and creating temporary tables.
SQL queries for creating the Redshift table, copying data to a temporary table, deleting records, and inserting records are defined.
If any step fails, it rolls back the transaction to maintain data consistency."""

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "reddit"

# Check the command line argument passed
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error {e}")
    sys.exit(1)

# Define S3 file path and role string
file_path = f"s3://{BUCKET_NAME}/{output_name}.csv"
role_string = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"

# SQL queries for Redshift data loading
create_table_query = sql.SQL(
    """CREATE TABLE IF NOT EXISTS {table} (
                            id varchar PRIMARY KEY,
                            title varchar(max),
                            num_comments int,
                            score int,
                            author varchar(max),
                            created_utc timestamp,
                            url varchar(max),
                            upvote_ratio float,
                            over_18 bool,
                            edited bool,
                            spoiler bool,
                            stickied bool
                        );"""
).format(table=sql.Identifier(TABLE_NAME))

create_temp_table_query = sql.SQL("CREATE TEMP TABLE our_staging_table (LIKE {table});").format(
    table=sql.Identifier(TABLE_NAME)
)

copy_to_temp_table_query = f"COPY our_staging_table FROM '{file_path}' iam_role '{role_string}' IGNOREHEADER 1 DELIMITER ',' CSV;"

delete_from_main_table_query = sql.SQL(
    "DELETE FROM {table} USING our_staging_table WHERE {table}.id = our_staging_table.id;"
).format(table=sql.Identifier(TABLE_NAME))

insert_into_main_table_query = sql.SQL(
    "INSERT INTO {table} SELECT * FROM our_staging_table;"
).format(table=sql.Identifier(TABLE_NAME))

drop_temp_table_query = "DROP TABLE our_staging_table;"

def main():
    """Upload file from S3 to Redshift Table"""
    validate_input(output_name)
    rs_conn = connect_to_redshift()
    load_data_into_redshift(rs_conn)

def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
        return rs_conn
    except Exception as e:
        print(f"Unable to connect to Redshift. Error: {e}")
        sys.exit(1)

def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    with rs_conn:
        with rs_conn.cursor() as cur:
            try:
                cur.execute(create_table_query)
                cur.execute(create_temp_table_query)
                cur.execute(copy_to_temp_table_query)
                cur.execute(delete_from_main_table_query)
                cur.execute(insert_into_main_table_query)
                cur.execute(drop_temp_table_query)
                rs_conn.commit()  # Commit the transaction if everything succeeds
            except Exception as e:
                rs_conn.rollback()  # Rollback in case of an exception
                print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()


