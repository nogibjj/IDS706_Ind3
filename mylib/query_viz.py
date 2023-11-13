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
    query = (
        "SELECT a.state, "
            "AVG(a.median_household_income) AS average_median_household_income, "
            "AVG(a.share_unemployed_seasonal) AS average_share_unemployed_seasonal, "
            "a.share_population_in_metro_areas, "
            "b.gini_index "
            "FROM default.hate_crimes1DB AS a "
            "JOIN default.hate_crimes2DB AS b ON a.id = b.id "
            "GROUP BY a.state, a.share_population_in_metro_areas, b.gini_index "
            "ORDER BY b.gini_index "
            "LIMIT 5"
    )
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    plt.figure(figsize=(15, 8))  # Adjusted figure size
    query.select("share_population_in_metro_areas", "state").toPandas().boxplot(
        column="share_population_in_metro_areas", by="state"
    )
    plt.xlabel("state")
    plt.ylabel("share_population_in_metro_areas")
    plt.suptitle("")
    plt.title("Share population in metro areas by State")
    # Adjust the rotation and spacing of x-axis labels
    plt.xticks(rotation=30, ha="right")  # ha='right' aligns the labels to the right
    plt.tight_layout()  # Ensures proper spacing
    plt.show("share_population_metro.png")

    query.select("average_median_household_income", "state").toPandas()

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar("state", "average_median_household_income", 
        color="blue"
    )
    plt.xlabel("State")
    plt.ylabel("Average Median Household Income")
    plt.title("Average Median Household Income by State")
    plt.xticks(rotation=45)
    plt.show("median_income.png")


if __name__ == "__main__":
    query_transform()
    viz()
    