# COMMAND ----------

'''
Create a dimensional data model, based on the transformed data, so that we can easily create dashboards for monitoring trends and doing quick analyses
'''
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

source_path = '/mnt/snp500/twitter/tweets_transformed'
destination_path = '/mnt/snp500/twitter/datamodel'

# COMMAND ----------

# Load the data from the transformed data table and create the dimensions

# Date dimension
date_dim = df.select(['date']).distinct()
date_dim = date_dim.withColumn(
  dim_id', monotonically_increasing_id())

# Create the user_dim dimension table (username and location dimensions)
user_dim = df.select(['username', 'location', 'followers_count']).distinct()
user_dim = user_dim.withColumn('dim_id', monotonically_increasing_id())

# Create the hashtags_dim dimension table (hashtags dimension)
hashtags_dim = df.select('hashtags').withColumn('dim_id', monotonically_increasing_id())
hashtags_dim = hashtags_dim.withColumn('hashtags', explode(hashtags_dim.hashtags))
hashtags_dim = hashtags_dim.select(['dim_id', 'hashtags']).distinct()

# COMMAND ----------

# Create the fact_table
fact_table = df.select(['date', 'username', 'hashtags', 'sentiment'])
fact_table = fact_table.join(date_dim, fact_table.date == date_dim.date)
fact_table = fact_table.join(user_dim, fact_table.username == user_dim.username)
fact_table = fact_table.join(hashtags_dim, fact_table.hashtags == hashtags_dim.hashtags)

# COMMAND ----------

# Select only the surrogate key columns from the dimensions
fact_table = fact_table.select(['date_dim_id', 'user_dim_id', 'hashtags_dim_id', 'sentiment'])
fact_table = fact_table.withColumnRenamed('date_dim_id', 'dim_id_1')
fact_table = fact_table.withColumnRenamed('user_dim_id', 'dim_id_2')
fact_table = fact_table.withColumnRenamed('hashtags_dim_id', 'dim_id_3')

# COMMAND ----------

# Write the dimension tables and fact table back to the delta lake
date_dim.write.format('delta').mode('overwrite').save(f'{destination_path}/date_dim')
user_dim.write.format('delta').mode('overwrite').save(f'{destination_path}/user_dim')
hashtags_dim.write.format('delta').mode('overwrite').save(f'{destination_path}/hashtags_dim')
fact_table.write.format('delta').mode('overwrite').save(f'{destination_path}/fact_table')
