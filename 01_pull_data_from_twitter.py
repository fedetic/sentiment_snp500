# COMMAND ----------

# Set up the Twitter API client
import tweepy
import os
import config
import delta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

consumer_key = config.CONSUMER_KEY
consumer_secret = config.CONSUMER_SECRET
access_token = config.ACCESS_TOKEN
access_token_secret = config.ACCESS_TOKEN_SECRET

auth = tweepy.OAuth1UserHandler(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)

api = tweepy.API(auth)

spark = SparkSession.builder.getOrCreate()

# Set up a Spark streaming context with a batch interval of 5 minutes
ssc = StreamingContext(sc, 300)

tweets_table_path = '/mnt/snp500/twitter/tweets'
profiles_table_path = '/dbfs/mnt/delta/twitter/profiles'

# COMMAND ----------

# Read the user's profile data from the Twitter API and write it to the Delta Lake table (profiles_table_path)
def update_profiles(user_ids):
  for user_id in user_ids:
    user = api.get_user(user_id)
    df = spark.createDataFrame([(user.id, user.screen_name, user.location, user.followers_count)], ['id', 'screen_name', 'location', 'followers_count'])
    df.write.format("delta").mode("append").save(profiles_table_path)

# COMMAND ----------

# Set up a Spark DStream to listen to the Twitter API
twitter_stream = tweepy.UserStream(auth=auth, listener=tweepy.Streaming.StatusStream)
filtered_stream = tweepy.Stream(auth=auth, listener=tweepy.Streaming.StatusStream).filter(track=['#S&P500'])

# Convert the DStream to a Spark DStream
spark_stream = ssc.queueStream([], default=filtered_stream)

# Convert the Spark DStream to a Spark DataFrame
df = spark_stream.map(lambda x: (x['id'], x['created_at'], x['text'], x['hashtags'], x['user']['id'], x['user']['screen_name'])).toDF(['id', 'created_at', 'text', 'hashtags', 'user_id', 'screen_name'])

# COMMAND ----------

# Write the df (tweets) to the Delta Lake table (tweets_table_path)
df.write.format("delta").mode("append").save(tweets_table_path)

# COMMAND ----------

# Set up a Spark streaming job to update the user profiles every hour
ssc.queueStream([], default=update_profiles).window(3600).foreachRDD(lambda rdd: rdd.foreach(lambda x: x))
