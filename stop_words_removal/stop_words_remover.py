from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('StopWordsRemover').getOrCreate()

from pyspark.ml.feature import StopWordsRemover, Tokenizer

data = spark.read.csv('../../.virtualenvs/spark_project1/databases/text.csv',header=True,inferSchema=True)

print("Initial Data")

data.show(truncate=False)

simple_tokenizer = Tokenizer(inputCol='text_content',outputCol='tokens_words')

simple_tokens = simple_tokenizer.transform(data)
print("Tokenizer Output - Splitting text on Whitespaces")
simple_tokens.show(truncate=False)

stopWords = StopWordsRemover(inputCol='tokens_words',outputCol='stopWordsRemoved')
stopWords_tokens = stopWords.transform(simple_tokens)
print("Data after Stop Words Removal")

stopWords_tokens.select('tokens_words','stopWordsRemoved').show(truncate=False)


