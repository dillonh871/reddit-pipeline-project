import configparser
import datetime
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType
from validation import validate_input  # Assuming this is an external validation function


"""The script is designed to extract Reddit data using the PRAW library and save it as a CSV file.
It first reads configuration variables (e.g., secret, client ID, etc.) from a configuration file.
The main function orchestrates the entire process, including connecting to the Reddit API, creating a Spark DataFrame, transforming the data, and saving it as a CSV.
The api_connect function establishes a connection to the Reddit API using PRAW.
The subreddit_posts function fetches posts from a specified subreddit, converts them to a Spark DataFrame, and returns the DataFrame.
The transform_basic function performs basic data transformations, such as converting timestamps to UTC and converting string columns to boolean.
The load_to_csv function writes the DataFrame to a CSV file.
In the script's main section, you specify the output file name and call the main function to perform the data extraction and transformation."""

# Read Configuration File
config = configparser.ConfigParser()
config.read("configuration.conf")

# Configuration Variables
SECRET = config.get("reddit_config", "secret")
CLIENT_ID = config.get("reddit_config", "client_id")

# Options for extracting data from PRAW
SUBREDDIT = "personalfinance"
TIME_FILTER = "day"
LIMIT = None

# Fields that will be extracted from Reddit
POST_FIELDS = [
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
]

# Use command line argument as the output file name
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Error with file input. Error {e}")
    sys.exit(1)

date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")


def main():
    """Extract Reddit data and load to CSV"""
    validate_input(output_name)
    spark = SparkSession.builder.appName("RedditData").getOrCreate()
    reddit_instance = api_connect()
    subreddit_posts_df = subreddit_posts(spark, reddit_instance)
    transformed_data = transform_basic(subreddit_posts_df)
    load_to_csv(transformed_data)


def api_connect():
    """Connect to Reddit API using PRAW"""
    try:
        import praw

        instance = praw.Reddit(
            client_id=CLIENT_ID, client_secret=SECRET, user_agent="My User Agent"
        )
        return instance
    except Exception as e:
        print(f"Unable to connect to the API. Error: {e}")
        sys.exit(1)


def subreddit_posts(spark, reddit_instance):
    """Create DataFrame for Reddit posts"""
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)

        # Convert PRAW submission objects to a Spark DataFrame
        posts_df = spark.createDataFrame(
            posts,
            schema=POST_FIELDS,
        )

        return posts_df
    except Exception as e:
        print(f"There's been an issue. Error: {e}")
        sys.exit(1)


def transform_basic(df):
    """Some basic transformation of data"""
    # Convert epoch to UTC
    df = df.withColumn("created_utc", col("created_utc").cast("timestamp"))

    # Convert string columns to boolean
    bool_columns = ["over_18", "edited", "spoiler", "stickied"]
    for col_name in bool_columns:
        df = df.withColumn(col_name, col(col_name).cast(BooleanType()))

    return df


def load_to_csv(df):
    """Save extracted data to a CSV file"""
    df.coalesce(1).write.mode("overwrite").csv(f"/tmp/{output_name}.csv", header=True)


if __name__ == "__main__":
    main()


