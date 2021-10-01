from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkFilters").getOrCreate()

df = spark.read.csv("../../Workspace/spark_project1/database/items_bought.csv",header=True,inferSchema=True)
df.show(4)
# Using Aggregate Function
df.agg({"total_amount":"sum"}).show()
df.agg({"total_amount":"max"}).show()
df.groupBy("item_name").agg({"total_amount":"sum"}).show()
