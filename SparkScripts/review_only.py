from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.functions import to_timestamp, col

spark = SparkSession.builder.appName("YelpReviewOnly").getOrCreate()

bucket = "gs://msba405-yelp-data"

review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("useful", LongType(), True),
    StructField("funny", LongType(), True),
    StructField("cool", LongType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True),
])

review_df = spark.read.schema(review_schema).json(
    f"{bucket}/raw/yelp/yelp_academic_dataset_review.json"
)

review_df = review_df.withColumn("date_ts", to_timestamp(col("date")))

review_df.write.mode("overwrite").parquet(
    f"{bucket}/processed/parquet/review"
)

spark.stop()