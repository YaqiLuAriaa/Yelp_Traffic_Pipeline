from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YelpBusinessOnly").getOrCreate()

bucket = "gs://msba405-yelp-data"

business_df = spark.read.json(
    f"{bucket}/raw/yelp/yelp_academic_dataset_business.json"
)

business_df.write.mode("overwrite").parquet(
    f"{bucket}/processed/parquet/business"
)

spark.stop()