# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/individual3/women_stem.csv", 
         dataset2="dbfs:/FileStore/individual3/recent_grads.csv"):
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
