from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
df = spark.read.csv("../../Workspace/spark_project1/database/employee.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("associates") #Creating a view

sql_result_1 = spark.sql("SELECT * FROM associates")
print("Showing the results of the select query")
sql_result_1.show()

sql_result_2 = spark.sql("SELECT * FROM associates WHERE age BETWEEN 30 AND 40 AND location='Alaska'")
print("Showing the results after using Where clause in select query")
sql_result_2.show()
