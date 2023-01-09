# COMMAND ----------

from pyspark.sql.functions import *
import nltk
from nltk.corpus import stopwords
import transformers

# COMMAND ----------

tweets_table_path = '/mnt/snp500/twitter/tweets'
profiles_table_path = '/dbfs/mnt/delta/twitter/profiles'

destination_path = '/mnt/snp500/twitter/tweets_transformed'

# COMMAND ----------

# Use a sentiment analysis model to classify the tweets as positive, negative, or neutral
model = transformers.BertForSequenceClassification.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')

def get_sentiment(text):
  input_ids = transformers.BertTokenizer.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment').encode(text, return_tensors='pt').to('cpu')
  output = model(input_ids).argmax().item()
  if output == 0:
    return 'negative'
  elif output == 1:
    return 'neutral'
  else:
    return 'positive'

# COMMAND ----------

# Read in the data from the Delta tables
tweets_df = spark.read.format("delta").load(tweets_table_path)
profiles_df = spark.read.format("delta").load(profiles_table_path)

# COMMAND ----------

# Join the data from the tweets and profiles tables
df = tweets_df.join(profiles_df, tweets_df.user_id == profiles_df.id)
df = df.select(['created_at', 'text', 'hashtags', 'screen_name', 'location', 'followers_count'])
df = df.withColumnRenamed('created_at', 'date')
df = df.withColumnRenamed('screen_name', 'username')

# COMMAND ----------

# Remove tweets with no hashtags or text
df = df.filter(df.hashtags.isNotNull() & df.text.isNotNull())

# Convert the hashtags column to an array of strings
df = df.withColumn('hashtags', split(df.hashtags, ','))

# Remove punctuation and special characters from the text column
df = df.withColumn('text', lower(regexp_replace(df.text, '[^\w\s]', '')))

# Remove stop words from the 'text' column
nltk.download('stopwords')
stop_words = stopwords.words('english')
df = df.withColumn('text', split(df.text, ' '))
df = df.withColumn('text', array_except(df.text, stop_words))
df = df.withColumn('text', concat_ws(' ', df.text))

# COMMAND ----------

# Use model to get sentiment
get_sentiment_udf = udf(get_sentiment, StringType())
df = df.withColumn('sentiment', get_sentiment_udf(df.text))

# COMMAND ----------

# Write the transformed data
df.write.format("delta").mode("overwrite").save(destination_path)
