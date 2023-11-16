# Databricks notebook source
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
    SELECT major_category AS category, major, SUM(men) AS total_men, 
           SUM(women) AS total_women, AVG(sharewomen) as avg_share_women
    FROM recentgrads_delta AS a 
    LEFT JOIN womenstem_delta AS b ON a.major = b.major
    GROUP BY category, major
    ORDER BY category, major 
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
        color='skyblue')
    plt.title("Average Share of Women in STEM for Each Category")
    plt.xlabel("category")
    plt.ylabel("average share of women")
    plt.show()

    # Plot 2
    plt.figure(figsize=(15, 7))
    query_result_pd.plot(
        x='major',
        y=['total_men', 'total_women'],
        kind='bar'
    )
    plt.title('Total Men vs. Women for Each Major')
    plt.ylabel('Counts')
    plt.xlabel('Major')
    plt.xticks(rotation=45)
    plt.legend(title='Metrics')
    plt.tight_layout()
    plt.show()    


if __name__ == "__main__":
    query_transform()
    viz()
