from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

spark = SparkSession.builder.appName("YelpUserOnly").getOrCreate()

bucket = "gs://msba405-yelp-data"

user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("review_count", LongType(), True),
    StructField("yelping_since", StringType(), True),
    StructField("useful", LongType(), True),
    StructField("funny", LongType(), True),
    StructField("cool", LongType(), True),
    StructField("elite", StringType(), True),
    StructField("friends", StringType(), True),
    StructField("fans", LongType(), True),
    StructField("average_stars", DoubleType(), True),
    StructField("compliment_hot", LongType(), True),
    StructField("compliment_more", LongType(), True),
    StructField("compliment_profile", LongType(), True),
    StructField("compliment_cute", LongType(), True),
    StructField("compliment_list", LongType(), True),
    StructField("compliment_note", LongType(), True),
    StructField("compliment_plain", LongType(), True),
    StructField("compliment_cool", LongType(), True),
    StructField("compliment_funny", LongType(), True),
    StructField("compliment_writer", LongType(), True),
    StructField("compliment_photos", LongType(), True),
])

user_df = spark.read.schema(user_schema).json(
    f"{bucket}/raw/yelp/yelp_academic_dataset_user.json"
)

user_df.write.mode("overwrite").parquet(
    f"{bucket}/processed/parquet/user"
)

spark.stop()