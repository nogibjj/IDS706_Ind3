"""
query and viz file
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

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
