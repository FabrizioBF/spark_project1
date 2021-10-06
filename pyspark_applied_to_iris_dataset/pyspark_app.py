from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('k_means').getOrCreate()

import pyspark
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.functions import rand, randn
from pyspark.ml.clustering import KMeans

df=spark.read.csv('../../.virtualenvs/spark_project1/databases/iris_dataset.csv',inferSchema=True,header=True)

print((df.count(),len(df.columns)))

df.columns

df.printSchema()

df.orderBy(rand()).show(6,False)

df.select('species').distinct().count()

df.groupBy('species').count().orderBy('count',ascending=False).show(6,False)
