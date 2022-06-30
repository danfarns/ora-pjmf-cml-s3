import cml.data_v1 as cmldata
from pyspark.sql import SparkSession

# Then call the `getOrCreate()` method of
# `SparkSession.builder` to start a Spark application.
# This example also gives the Spark application a name:
CONNECTION_NAME = ""
S3A_AMAZON_BUCKET_FILE = ""

if CONNECTION_NAME == "":
	raise Exception("Please update the variable [CONNECTION_NAME] with the connection name you were given.") 

if S3A_AMAZON_BUCKET_FILE == "":
	raise Exception("Please update the variable [S3A_AMAZON_BUCKET_FILE] with the location of the file you want to grab from your S3 Bucket.") 

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
# Now you can use the `SparkSession` named `spark` to read
# data into Spark.
# ## Reading Data
# Read the flights dataset. This data is in CSV format
# and includes a header row. Spark can infer the schema
# automatically from the data:
flights = spark.read.csv(S3A_AMAZON_BUCKET_FILE, header=True, inferSchema=True)

# The result is a Spark DataFrame named `flights`.
# ## Inspecting Data
# Inspect the DataFrame to gain a basic understanding
# of its structure and contents.
# Print the number of rows:
flights.count()
# Print the schema:
flights.printSchema()
# Inspect one or more variables (columns):
flights.describe('arr_delay').show()
flights.describe('arr_delay', 'dep_delay').show()
# Print five rows:
flights.limit(5).show()
# Or more concisely:
flights.show(5)
# Print 20 rows (the default number is 20):
flights.show()


from pyspark.sql.functions import col, lit

flights.filter(col('dest') == lit('SFO')).show()

# `orderBy()` (or its alias `sort()`) returns rows
# arranged by the specified columns:

flights.orderBy('month', 'day').show()

flights.orderBy('month', 'day', ascending=False).show()

# `withColumn()` adds a new column or replaces an existing
# column using the specified expression:

flights \
  .withColumn('on_time', col('arr_delay') <= 0) \
  .show()

# To concatenate strings, import and use the function
# `concat()`:

from pyspark.sql.functions import concat

flights \
  .withColumn('flight_code', concat('carrier', 'flight')) \
  .show()

# `agg()` performs aggregations using the specified
# expressions.

# Import and use aggregation functions such as `count()`,
# `countDistinct()`, `sum()`, and `mean()`:

from pyspark.sql.functions import count, countDistinct

flights.agg(count('*')).show()

flights.agg(countDistinct('carrier')).show()

# Use the `alias()` method to assign a name to name the
# resulting column:

flights \
  .agg(countDistinct('carrier').alias('num_carriers')) \
  .show()

# `groupBy()` groups data by the specified columns, so
# aggregations can be computed by group:

from pyspark.sql.functions import mean

flights \
  .groupBy('origin') \
  .agg( \
       count('*').alias('num_departures'), \
       mean('dep_delay').alias('avg_dep_delay') \
  ) \
  .show()

# By chaining together multiple DataFrame methods, you
# can analyze data to answer questions. For example:

# How many flights to SFO departed from each airport,
# and what was the average departure delay (in minutes)?

flights \
  .filter(col('dest') == lit('SFO')) \
  .groupBy('origin') \
  .agg( \
       count('*').alias('num_departures'), \
       mean('dep_delay').alias('avg_dep_delay') \
  ) \
  .orderBy('avg_dep_delay') \
  .show()


# ## Using SQL Queries

# Instead of using Spark DataFrame methods, you can
# use a SQL query to achieve the same result.

# First you must create a temporary view with the
# DataFrame you want to query:

flights.createOrReplaceTempView('nyc_flights_2013')

# Then you can use SQL to query the DataFrame:

spark.sql("""
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM nyc_flights_2013
  WHERE dest = 'SFO'
  GROUP BY origin
  ORDER BY avg_dep_delay""").show()


# ## Cleanup

# Stop the Spark application:

spark.stop()