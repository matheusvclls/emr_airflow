# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp

# Create a spark session
spark = SparkSession.builder \
.appName(f'Modeling_data') \
.enableHiveSupport() \
.getOrCreate()

# Set bucket name
BUCKET_NAME = '<nome-do-seu-bucket>' 

# Read data from s3
df = spark.read.json("s3://{}/data/landing_zone/profile_user.json".format(BUCKET_NAME)).fillna('')

# Change birthdate from string to date
df = df.withColumn("birthdate",to_date(col("birthdate"),"yyyy-MM-dd"))

# Insert new column with processing date
df = df.withColumn("processing_date",current_timestamp())

# Write the dataframe treated as parquet type
df.write.parquet('s3://{}/data/processed_zone/profile_user.parquet'.format(BUCKET_NAME), mode='overwrite')