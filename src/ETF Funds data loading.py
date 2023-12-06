# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in AWS S3. There are two ways to establish access to S3: [IAM roles](https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html) and access keys.
# MAGIC
# MAGIC *We recommend using IAM roles to specify which cluster can access which buckets. Keys can show up in logs and table metadata and are therefore fundamentally insecure.* If you do use keys, you'll have to escape the `/` in your keys with `%2F`.
# MAGIC
# MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "s3://fdss3-dbis-databricks-etl/jcostamartin"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "|"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "temp"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `temp`

# COMMAND ----------

# Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "dbis_etl.etf_funds.etf_funds"

df.write.format("delta").mode("overwrite").saveAsTable(permanent_table_name)

# COMMAND ----------


