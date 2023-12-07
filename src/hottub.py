from pyspark.sql import SparkSession

# initiate a SparkSession
spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

# load data from CSV file to DataFrame
df = spark.read.format('parquet').options(header='true', inferSchema='true').load('s3://fdss3-silver-hot-tub-dev/extracts/ETF_V1/ETF_HOLDINGS_DETAIL/20231104200449/ETF_V1_ETF_HOLDINGS_DETAIL_20231104200449_full.parquet')

# transform DataFrame as necessary 
# since this is an example, I'm not performing any transformations
# df_transformed = df.select(...)

# write DataFrame to a Delta table
permanent_table_name = "dbis_etl.hottub.etf_v1"
df.write.format("delta").mode("overwrite").saveAsTable(permanent_table_name)

df = spark.read.format('parquet').options(header='true', inferSchema='true').load('s3://fdss3-silver-hot-tub-dev/extracts/ETF_V1/ETF_HOLDINGS_DETAIL/20231104200449/ETF_V1_ETF_HOLDINGS_DETAIL_20231104200449_full.parquet')