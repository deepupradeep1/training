# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

df=spark.read.option('header', True).option('inferSchema',True).csv('/mnt/formula1dl/raw/races.csv')
df.printSchema()

# COMMAND ----------



# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1dl/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

display(races_with_timestamp_df)                                  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

                                                 

# COMMAND ----------

temp=races_selected_df.where(col('race_year') == 2019).orderBy('race_id')
display(temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema   races

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl/output/races

# COMMAND ----------

races_selected_df.write.mode('overwrite').option('path','/mnt/formula1dl/output/races').saveAsTable('races.races_ext')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table races.races 

# COMMAND ----------

df=spark.sql("select * from races.races_ext")
display(df)
