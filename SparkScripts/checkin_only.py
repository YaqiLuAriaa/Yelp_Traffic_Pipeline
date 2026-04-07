from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("YelpCheckinOnly").getOrCreate()

bucket = "gs://msba405-yelp-data"

checkin_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("date", StringType(), True),
])

checkin_df = spark.read.schema(checkin_schema).json(
    f"{bucket}/raw/yelp/yelp_academic_dataset_checkin.json"
)

checkin_df.write.mode("overwrite").parquet(
    f"{bucket}/processed/parquet/checkin"
)

spark.stop()