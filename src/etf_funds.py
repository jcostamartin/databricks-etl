from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# initiate a SparkSession
spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

# load data from CSV file to DataFrame
csv_file_path = "s3://fdss3-dbis-databricks-etl/jcostamartin/etf_funds.csv"

schema = StructType([
    StructField("FACTSET_ENTITY_ID", StringType(), True),
    StructField("FSYM_ID", StringType(), True),
    StructField("REPORT_DATE", DateType(), True),
    StructField("SHARES", DoubleType(), True),
    StructField("PRICE", DoubleType(), True),
    StructField("MARKET_VALUE", DoubleType(), True),
    StructField("ISO_CURRENCY", StringType(), True),
    StructField("MV_WEIGHT", DoubleType(), True)
])

#df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter", "|").load(csv_file_path)
df = spark.read.format("csv").option("mergeSchema", "false").schema(schema).option("header", "true").option("delimiter", "|").load(csv_file_path)

# transform DataFrame as necessary 
# since this is an example, I'm not performing any transformations
# df_transformed = df.select(...)

# write DataFrame to a Delta table
permanent_table_name = "dbis_etl.etf_funds.etf_funds"
df.write.format("delta").mode("overwrite").saveAsTable(permanent_table_name)