[![CI](https://github.com/nogibjj/IDS706_Individual3/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/IDS706_Individual3/actions/workflows/cicd.yml)

## Individual Project 3: Data ETL (Extract, Transform, Load) Pipeline with Databricks

## Overview 

The "Data Extraction and Transformation Pipeline" project is centered around analyzing and processing two specific datasets: statistics about women in STEM and recent graduate data. This project leverages the power of Databricks, supplemented by its API and various Python libraries, to construct a specialized Databricks ETL (Extract, Transform, Load) Pipeline. This pipeline is tailored to manage and analyze these two datasets effectively.

Key components of this project include:

- ETL Notebook Documentation: Provides a comprehensive guide for the ETL operations, tailored to the two datasets.
- Delta Lake: Used for efficient and reliable data storage.
- Spark SQL: Employed for sophisticated data transformation processes.
- Data Visualizations: Created to extract actionable insights from the women in STEM statistics and recent graduate data.
- Data Integrity Assurance: Achieved through rigorous error handling and data validation procedures, ensuring accuracy and reliability.
- Automation and Continuous Processing: Featured through the automated Databricks API trigger, facilitating ongoing data processing.

The project's workflow is streamlined with a Makefile, offering a range of commands like make install for installation, make test for testing, make format using Python Black for code formatting, and make lint with Ruff for linting. Additionally, the integration of Github Actions for an all-encompassing task (make all) boosts code quality and smoothens the data analysis process.

## Dataset 
The dataset, which includes 'recent_grads.csv' and 'women_stem.csv,' originates from FiveThirtyEight and is based on the American Community Survey 2010-2012 Public Use Microdata Series. It provides detailed information about the percentage of women in each major category, specific majors, as well as the total number of graduates for both men and women.

[Dataset used](https://github.com/fivethirtyeight/data/tree/master/college-majors)

## Steps
- **Data Extraction:**
Utilizes the requests library to fetch health crime data from specified URLs. Downloads and stores the data in the Databricks FileStore.

- **Databricks Environment Setup:**
Establishes a connection to the Databricks environment using environment variables for authentication (SERVER_HOSTNAME and ACCESS_TOKEN).

- **Data Transformation and Load:**
Transform the csv file into a Spark dataframe which is then converted into a Delta Lake Table and stored in the Databricks environement.

- **Query Transformation and Vizulization:**
Defines a Spark SQL query to perform a predefined transformation on the retrieved data. Uses the predifined transformation Spark dataframe to create vizualizations.

- **File Path Checking for make test:**
Implements a function to check if a specified file path exists in the Databricks FileStore. As the majority of the functions only work exclusively in conjunction with Databricks, the Github environment cannot replicate and do not have access to the data in the Databricks workspace. I have opted to test whether I can still connect to the Databricks API. Utilizes the Databricks API and the requests library.

- **Automated trigger via Github Push:** 
I utilize the Databricks API to run a job on my Databricks workspace such that when a user pushes to this repo it will intiate a job run.

## Databricks Environment Setup 
1. Create a Databricks workspace on Azure
2. Connect Github account to Databricks Workspace 
3. Create global init script for cluster start to store enviornment variables 
4. Create a Databricks cluster that supports Pyspark
5. Clone repo into Databricks workspace
6. Create a job on Databricks to build pipeline

## Delta Source, Delta Sink (Delta Lake), and ETL Pipeline 
![image](https://github.com/nogibjj/IDS706_Individual3/assets/141780408/433bd43e-2a55-4c2d-b596-d49844f3b55e)

## Visualization of Query Result
![image](https://github.com/nogibjj/IDS706_Individual3/assets/141780408/c07d6ef0-0beb-4bb0-abe5-0c1aefc7d58c)
![image](https://github.com/nogibjj/IDS706_Individual3/assets/141780408/4c0e61c5-d74d-4c54-9b02-23d364c5f31e)



## References
1. https://github.com/nogibjj/python-ruff-template
2. https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html
