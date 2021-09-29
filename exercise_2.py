from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Rows&Columns").getOrCreate()
df = spark.read.csv("../spark_project1/database/employee.csv",header=True)
df.show()
print("Multiple Column Data")
df.select(["employee_name","age"]).show()
