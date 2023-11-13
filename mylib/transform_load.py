from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/mini_project11/hate_crimes.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    hate_crimes_df = spark.read.csv(dataset, header=True, inferSchema=True)

    columns = hate_crimes_df.columns

    # Split columns into two halves
    columns1 = columns[:6]
    columns2 = columns[1,6,7,8,9,10]

    # Create two new DataFrames
    hate_crimes_df1 = hate_crimes_df.select(*columns1)
    hate_crimes_df2 = hate_crimes_df.select(*columns2)

    # add unique IDs to the DataFrames
    hate_crimes_df1 = hate_crimes_df1.withColumn(
        "id", monotonically_increasing_id()
    )
    hate_crimes_df2 = hate_crimes_df2.withColumn(
        "id", monotonically_increasing_id()
    )

    # transform into a delta lakes table and store it
    hate_crimes_df1.write.format("delta").mode("overwrite").saveAsTable(
        "hate_crimes_df1_delta"
    )
    hate_crimes_df2.write.format("delta").mode("overwrite").saveAsTable(
        "hate_crimes_df2_delta"
    )
    
    num_rows = hate_crimes_df1.count()
    print(num_rows)
    num_rows = hate_crimes_df2.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()