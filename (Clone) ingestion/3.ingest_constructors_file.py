# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema users

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1dl/raw/constructors.json")

# COMMAND ----------

df=spark.read.json("/mnt/formula1dl/raw/constructors.json")
# #display(df.where("nationality='British'")
#         .select("constructorId","constructorRef","name","nationality"))
        
display(df.filter("nationality='British'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = df.drop(col('url'))
display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/constructors")

# COMMAND ----------



# COMMAND ----------

json_df=spark.read.option("multiline","true").json("/mnt/formula1dl/users/airports.json")

display(json_df)
