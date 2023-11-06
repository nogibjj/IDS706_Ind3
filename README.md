[![CI](https://github.com/nogibjj/IDS706_Mini10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/IDS706_Mini10/actions/workflows/cicd.yml)

# Mini Project10: PySpark Data Processing

## Purpose
The project entails the utilization of PySpark for handling a sizable dataset. Its primary goals include implementing a Spark SQL query and carrying out a data transformation. I utilized the cereal dataset available on Kaggle for these tasks.

Dataset used: [80 Cereals](https://www.kaggle.com/datasets/crawford/80-cereals)

## Requirements
  * Use PySpark to perform data processing on a large dataset
  * Include at least one Spark SQL query and one data transformation

## Functionality
The code does data processing with Spark SQL and transformations:
  * [E] Extract a dataset from a URL with CSV format.
  * [T] Transform the data by filtering to get it ready for analysis.
  * [L] Load the transformed data into a database table using Spark SQL.
  * [Q] Accept and execute SQL queries on the database to analyze and retrieve insights from the data.

## Steps
1. open codespaces
2. wait for the environment to be installed
3. run: `python main.py`
4. [Pyspark Output Data/Summary Markdown File](pyspark_output.md)

## Check Format and Test errors
1. Format code: `make format`
2. Lint code: `make lint`
3. Test code: `make test`
