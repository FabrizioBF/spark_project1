from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Tokenizer').getOrCreate()

from pyspark.ml.feature import Tokenizer,RegexTokenizer

data = spark.read.csv('../../.virtualenvs/spark_project1/databases/text.csv',header=True,inferSchema=True)

print("Initial Data")

data.show(truncate=False)

simple_tokenizer = Tokenizer(inputCol='text_content',outputCol='tokens_words')

simple_tokens = simple_tokenizer.transform(data)

print("Tokenizer Output - Splitting text on Whitespaces")

simple_tokens.show(truncate=False)

regex_tokenizer = RegexTokenizer(inputCol='text_content',outputCol='tokens_words ',pattern='\\W')

regex_tokens = regex_tokenizer.transform(data)

print("RegexTokenizer Output - Splitting text on special sequence \W")

regex_tokens.show(truncate=False)
