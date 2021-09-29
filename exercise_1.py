from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyFirstSparkSession").getOrCreate()
df = spark.read.csv("../spark_project1/database/employee.csv",header=True)
df.show()
df.printSchema()
