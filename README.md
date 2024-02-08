# ny_taxi_tripdata_metrics

A simple Apache Airflow pipeline that gets tripdata metrics from a Clickhouse database and writes the results into a table in an SQLite database.

## Tools, Technologies and Libraries

1. Docker: Container technology for easy setup and running of our Airflow jobs.
2. Apache Airflow: A robust orchestration tool used to run the data pipeline.
3. SQL: Query language used to fetch reports from the database.
4. Clickhouse: An open source database column database and management system for real time analytics and blazing fast queries.
5. Python Libraries: pandas, sqalchemy, requests.

## Prerequisites

You need to have docker installed. See [here](https://docs.docker.com/engine/install/) for installation instructions.

## Set up Instructions

After cloning the repository, cd into the folder and run the following commands:

To build the Airflow image with the custom config in the Dockerfile, run the following:
`docker build .`

To run migrations and initialise the database Airflow will use:
`docker compose up airflow-init`

If you run into any errors, run `./airflow.sh db init`.

To run the container, `docker compose up`

## Usage

If all goes well above, visit your browser at <localhost:8080> and login with 'airflow' as username and password.

You can then click on the `moniepoint_write_metrics_to_sqlite` DAG and proceed to run the pipeline in the project.
