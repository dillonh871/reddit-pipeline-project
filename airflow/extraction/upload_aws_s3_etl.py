import boto3
import botocore
import configparser
import pathlib
import sys
from validation import validate_input  # Assuming this is an external validation function

"""This script is designed to upload a file to an AWS S3 bucket.
It uses command-line arguments for specifying the output file name.
Configuration variables and S3 connection are set up using boto3.
The main function orchestrates the process, creating the S3 bucket if it doesn't exist and uploading the file.
Functions for creating an S3 bucket if it doesn't exist and uploading a file to the bucket are defined.
It provides informative prints for tracking the upload process."""

def main():
    try:
        output_name = sys.argv[1]
    except Exception as e:
        print(f"Command line argument not passed. Error: {e}")
        sys.exit(1)
    
    validate_input(output_name)
    
    parser = configparser.ConfigParser()
    script_path = pathlib.Path(__file__).parent.resolve()
    config_file = f"{script_path}/configuration.conf"
    parser.read(config_file)
    
    BUCKET_NAME = parser.get("aws_config", "bucket_name")
    AWS_REGION = parser.get("aws_config", "aws_region")
    
    conn = boto3.resource("s3")
    create_bucket_if_not_exists(conn, BUCKET_NAME, AWS_REGION)
    upload_file_to_s3(conn, output_name, BUCKET_NAME)

def create_bucket_if_not_exists(conn, bucket_name, region):
    """Check if an S3 bucket exists and create it if not"""
    try:
        conn.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            conn.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )

def upload_file_to_s3(conn, output_name, bucket_name):
    """Upload a file to an S3 bucket"""
    file_name = f"{output_name}.csv"
    source_file = f"/tmp/{file_name}"
    key = file_name
    conn.meta.client.upload_file(Filename=source_file, Bucket=bucket_name, Key=key)
    print(f"Uploaded {source_file} to S3 bucket {bucket_name} as {key}")

if __name__ == "__main__":
    main()
