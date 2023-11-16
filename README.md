# Individual Project3: Data Pipeline with Databricks

## Overview 
The Data Extraction and Transformation Pipeline project is designed to extract and process tennis match data from the fivethirtyeight datasets. It leverages Databricks in combination with the Databricks API and various Python libraries to achieve this goal. Additionally, this project involves the development of a comprehensive Databricks ETL (Extract, Transform, Load) Pipeline for retrieving and processing airline safety datasets.

Key features of this project include a well-documented notebook for ETL operations, the utilization of Delta Lake for efficient data storage, Spark SQL for data transformations, and data visualizations for actionable insights. Data integrity is a top priority, ensured through robust error handling and data validation procedures. The project places a strong emphasis on automation and continuous processing, as evidenced by the automated Databricks API trigger.

To streamline the workflow, a Makefile is implemented, offering tasks such as installation (`make install`), testing (`make test`), code formatting (`make format`) using Python Black, and linting (`make lint`) with Ruff. The use of Github Actions for an all-inclusive task (`make all`) further enhances code quality and ensures a seamless data analysis process.

## Dataset 
The dataset, which includes 'recent_grads.csv' and 'women_stem.csv,' originates from FiveThirtyEight and is based on the American Community Survey 2010-2012 Public Use Microdata Series. It provides detailed information about the percentage of women in each major category, specific majors, as well as the total number of graduates for both men and women.

[Dataset used](https://github.com/fivethirtyeight/data/tree/master/college-majors)

## Steps
- **Data Extraction:**
Utilizes the requests library to fetch health crime data from specified URLs. Downloads and stores the data in the Databricks FileStore.

- **Databricks Environment Setup:**
Establishes a connection to the Databricks environment using environment variables for authentication (SERVER_HOSTNAME and ACCESS_TOKEN).

- **Data Transformation and Load:**
Transform the csv file into a Spark dataframe which is then converted into a Delta Lake Table and stored in the Databricks environement

- **Query Transformation and Vizulization:**
Defines a Spark SQL query to perform a predefined transformation on the retrieved data. Uses the predifined transformation Spark dataframe to create vizualizations

- **File Path Checking for make test:**
Implements a function to check if a specified file path exists in the Databricks FileStore. As the majority of the functions only work exclusively in conjunction with Databricks, the Github environment cannot replicate and do not have access to the data in the Databricks workspace. I have opted to test whether I can still connect to the Databricks API. Utilizes the Databricks API and the requests library.

- **Automated trigger via Github Push:** 
I utilize the Databricks API to run a job on my Databricks workspace such that when a user pushes to this repo it will intiate a job run

## Databricks Environment Setup 
1. Create a Databricks workspace on Azure
2. Connect Github account to Databricks Workspace 
3. Create global init script for cluster start to store enviornment variables 
4. Create a Databricks cluster that supports Pyspark
5. Clone repo into Databricks workspace
6. Create a job on Databricks to build pipeline

## Delta Source, Delta Sink (Delta Lake), and ETL Pipeline 

## Visualization of Query Result

## References
1. https://github.com/nogibjj/python-ruff-template
2. https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html
