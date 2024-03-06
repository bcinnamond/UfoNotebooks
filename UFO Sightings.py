# Databricks notebook source
# MAGIC %md
# MAGIC #### Connecting the Azure Blob Storage container

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://ufo-sighting-data@rcmoviesdatasa.blob.core.windows.net",
    mount_point="/mnt/ufo-sighting-data",
    extra_configs={
        "fs.azure.account.key.rcmoviesdatasa.blob.core.windows.net": dbutils.secrets.get(
            "projectmoviesscope", "storageAccountKey"
        )
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using the %fs command to list the files and folders that are mounted within the ufo-sighting-data storage container

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/ufo-sighting-data"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Using Spark to read the CSV file. It loads the CSV into a variable called "ufo" for later use

# COMMAND ----------

ufo = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/mnt/ufo-sighting-data/raw-data/ufo-sightings-transformed.csv")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Display 15 records from ufo DataFrame

# COMMAND ----------

ufo.limit(15).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the structure of the ufo DataFrame including columns and their data types

# COMMAND ----------

ufo.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Importing PySpark functions for referencing columns and defining data types

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ensure that values in the "Encounter_Duration" column of the DataFrame ufo are interpreted as integers

# COMMAND ----------

ufo = ufo.withColumn("Encounter_Duration", col("Encounter_Duration").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cast the "Year" coumn to DateType is not appropriate because "Year" is typically presented as an integer in datasets

# COMMAND ----------

ufo = ufo.withColumn("Year", col("Year").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert the "date_documented" colum to data format, the new column is derived from the existing one but with different data type

# COMMAND ----------

from pyspark.sql import functions as F
ufo= ufo.withColumn('date_documented',F.to_date(ufo.date_documented))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save new column to the dataframe

# COMMAND ----------

ufo.write.mode("overwrite").option("header",'true').csv("/mnt/ufo-data/transformed/ufo")

# COMMAND ----------

# MAGIC %md
# MAGIC #### By changing this columns data type, we can now run SQL queries using Spark SQL

# COMMAND ----------

# Register the DataFrame as a temporary table to run SQL queries
ufo.createOrReplaceTempView("ufo_table")

# Example query: Get the count of UFO sightings per country
result = spark.sql("""
    SELECT Country, COUNT(*) as CountOfSightings
    FROM ufo_table
    GROUP BY Country
    ORDER BY CountOfSightings DESC
""")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Three quotation marks are used to show multi-line strings in Python

# COMMAND ----------

# Example query: Find the average length of UFO encounters per UFO shape
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")
result.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### You can also display the results in bar chart format by using display command

# COMMAND ----------

ufo.createOrReplaceTempView("ufo_table")

# Example query: Find the average length of UFO encounters per UFO shape
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")

# Display the result as a bar chart
display(result)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Visualisation examples

# COMMAND ----------

# Count the occurrences of each UFO shape
shape_counts = ufo.groupBy("UFO_shape").count().orderBy("count", ascending=False)

# Display the distribution of UFO shapes as a pie chart
display(shape_counts)

# COMMAND ----------

# Count the number of UFO sightings per country
sightings_per_country = ufo.groupBy("Country").count().orderBy("count", ascending=False)

# Display the UFO sightings count per country as a bar chart
display(result)

# COMMAND ----------

from pyspark.sql.functions import desc

# Find the longest UFO encounter for each country
longest_encounter_per_country = ufo.groupBy("Country").agg({"length_of_encounter_seconds": "max"}) \
    .withColumnRenamed("max(length_of_encounter_seconds)", "Longest_Encounter_Seconds") \
    .orderBy(desc("Longest_Encounter_Seconds"))

# Display the longest UFO encounters per country as a bar chart
display(longest_encounter_per_country)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Filter data to remove missing or invalid location data

# COMMAND ----------

# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### *The visualisation above does not work due to the column data types not being in the correct format for map plotting

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Longtitute and latitude fields should be represented as floating-point numbers. We need to cast these columns as Double to ensure they are the correct format for analysis and visualisation

# COMMAND ----------

ufo = ufo.withColumn("latitude", col("latitude").cast(DoubleType()))
ufo = ufo.withColumn("longitude", col("longitude").cast(DoubleType()))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Overwrite the DataFrame to ensure column changes are saved

# COMMAND ----------

ufo.write.mode("overwrite").option("header",'true').csv("/mnt/ufo-data/transformed/ufo")

# COMMAND ----------

# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select necessary columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Added cluster markers to show the number of sightings per location
# MAGIC Visualisation > Style tab > Cluster Markers on 

# COMMAND ----------

# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select necessary columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)
