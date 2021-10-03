from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('TF-IDF_HashTF').getOrCreate()

from pyspark.ml.feature import Tokenizer,HashingTF,IDF
data = spark.read.csv('../../.virtualenvs/spark_project1/databases/reviews.csv',header=True,inferSchema=True)
print("Initial Data")

data.show(truncate=False)

simple_tokenizer = Tokenizer(inputCol='reviews',outputCol='review_tokens')
simple_tokens = simple_tokenizer.transform(data)
print("Tokenizer Output - Splitting text on Whitespaces")
simple_tokens.show(truncate=False)

hashingtf_vectors = HashingTF(inputCol='review_tokens',outputCol='hashVec')
HashingTF_featurized_data = hashingtf_vectors.transform(simple_tokens)
print("HashingTF Data")
HashingTF_featurized_data.select('review_tokens','hashVec').show(truncate=40)

idf = IDF(inputCol='hashVec',outputCol='features')
idf_model = idf.fit(HashingTF_featurized_data)
final_data = idf_model.transform(HashingTF_featurized_data)
print("Formatted Data")
final_data.select('label','features').show(truncate=60)
