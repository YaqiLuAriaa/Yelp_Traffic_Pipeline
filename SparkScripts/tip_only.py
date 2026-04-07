from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import to_timestamp, col

spark = SparkSession.builder.appName("YelpTipOnly").getOrCreate()

bucket = "gs://msba405-yelp-data"

tip_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True),
    StructField("compliment_count", LongType(), True),
])

tip_df = spark.read.schema(tip_schema).json(
    f"{bucket}/raw/yelp/yelp_academic_dataset_tip.json"
)

tip_df = tip_df.withColumn("date_ts", to_timestamp(col("date")))

tip_df.write.mode("overwrite").parquet(
    f"{bucket}/processed/parquet/tip"
)

spark.stop()