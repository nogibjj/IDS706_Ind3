# Databricks notebook source
!pip install -r ../requirements.txt

import requests
from dotenv import load_dotenv
import os
import json
import base64
import seaborn as sns

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/Individual3"
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', url + path, 
                           data=json.dumps(data), 
                           verify=True, 
                           headers=headers)
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data['path'] = path
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)
  

def create(path, overwrite, headers):
    _data = {}
    _data['path'] = path
    _data['overwrite'] = overwrite
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data['handle'] = handle
    _data['data'] = data
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data['handle'] = handle
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, 
                      base64.standard_b64encode(content[i:i+2**20]).decode(), 
                      headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
        url="""https://github.com/fivethirtyeight/data/blob/master/college-majors/women-stem.csv?raw=true""",
        url2="""https://github.com/fivethirtyeight/data/blob/master/college-majors/recent-grads.csv?raw=true""",
        file_path=FILESTORE_PATH+"/women_stem.csv",
        file_path2=FILESTORE_PATH+"/recent_grads.csv",
        directory=FILESTORE_PATH,
        overwrite=True
):
    """Extract a url to a file path"""
    # Make the directory, no need to check if it exists or not
    mkdirs(path=directory, headers=headers)
    # Add the csv files, no need to check if it exists or not
    put_file_from_url(url, file_path, overwrite, headers=headers)
    put_file_from_url(url2, file_path2, overwrite, headers=headers)

    return file_path


if __name__ == "__main__":
    extract()

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/Individual3/women_stem.csv", 
         dataset2="dbfs:/FileStore/Individual3/recent_grads.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    women_stem_df = spark.read.csv(dataset, header=True, inferSchema=True)
    recent_grads_df = spark.read.csv(dataset2, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    women_stem_df = women_stem_df.withColumn("id", monotonically_increasing_id())
    recent_grads_df = recent_grads_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    recent_grads_df.write.format("delta").mode("overwrite").saveAsTable("recentgrads_delta")
    women_stem_df.write.format("delta").mode("overwrite").saveAsTable("womenstem_delta")
    
    num_rows = recent_grads_df.count()
    print(num_rows)
    num_rows = women_stem_df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()

"""
query and viz file
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = """
    SELECT a.major_category AS category, a.major, SUM(a.men) AS total_men, 
           SUM(a.women) AS total_women, AVG(a.sharewomen) as avg_share_women
    FROM womenstem_delta AS a 
    INNER JOIN recentgrads_delta AS b ON a.major = b.major
    GROUP BY a.major_category, a.major
    ORDER BY a.major_category, a.major 
    """
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz():
    query_result = query_transform()
    count = query_result.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    query_result_pd = query_result.toPandas()

    # Plot 1
    plt.figure(figsize=(15, 8))
    plt.bar(
        query_result_pd["category"], 
        query_result_pd["avg_share_women"], 
        color='orange')
    plt.title("Average Share of Women in STEM by Major Category")
    plt.xlabel("Major Category")
    plt.xticks(rotation=45)  
    plt.ylabel("Average Share of Women")
    plt.show()

    # Plot 2
    plt.figure(figsize=(15, 7))
    sns.barplot(
        y="category", 
        x="value",
        hue="gender", 
        data=query_result_pd.melt(id_vars='category', 
                                  value_vars=['total_men', 'total_women'], 
                                  var_name='gender', value_name='value'),
        palette=['blue', 'pink'], ci=None
    )
    plt.title('Total Men vs. Women by Major Category in STEM')
    plt.xlabel('Total Conts')
    plt.ylabel('Major Category')
    plt.xticks(fontsize=10)
    plt.legend(title='Gender')
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    query_transform()
    viz()

