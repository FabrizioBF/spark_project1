from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkFilters").getOrCreate()
df = spark.read.csv("../../Workspace/spark_project1/database/items_bought.csv",header=True,inferSchema=True)
df.show(4)

print("Result after filtering total_amount greater than 1000")
df.filter("total_amount>1000").show()

print("Result after filtering based on item_price and tax_amount")
df.filter((df["item_price"]>1000)&(df["tax_amount"]>500)).show()
