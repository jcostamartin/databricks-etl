from pyspark.sql import SparkSession

# initiate a SparkSession
spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

# load data from CSV file to DataFrame
csv_file_path = "s3://fdss3-dbis-databricks-etl/jcostamartin"
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(csv_file_path)

# transform DataFrame as necessary 
# since this is an example, I'm not performing any transformations
# df_transformed = df.select(...)

# write DataFrame to a Delta table
permanent_table_name = "dbis_etl.etf_funds.etf_funds"
df.write.format("delta").mode("overwrite").saveAsTable(permanent_table_name)