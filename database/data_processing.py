from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('data_processing').getOrCreate()

df=spark.read.csv('../../.virtualenvs/spark_project1/databases/sample_data.csv',inferSchema=True,header=True)

df.columns

len(df.columns)

df.count()

df.printSchema()

df.show(4)

df.select('age','mobile').show(4)

df.describe().show()

df.groupBy('mobile').count().show(4,False)

df.groupBy('mobile').count().orderBy('count',ascending=False).show(4,False)
