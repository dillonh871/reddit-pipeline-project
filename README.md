# Reddit ETL Pipeline

A data pipeline to extract Reddit data from subreddits. etc (/personalfinance, /wallstreetbets, /dataengineering)

1. Extract data using [Reddit API](https://www.reddit.com/dev/api/)
1. Orchestrate with [Airflow](https://airflow.apache.org) in [Docker](https://www.docker.com)
1. Create AWS resources with [Terraform](https://www.terraform.io) utilizing S3 and Redshift
1. Load into [AWS S3](https://aws.amazon.com/s3/)
1. Copy into [AWS Redshift](https://aws.amazon.com/redshift/)
1. Transform using data using [dbt](https://www.getdbt.com)
1. Visualize data with from redshift with [Google Data Studio](https://datastudio.google.com) 
