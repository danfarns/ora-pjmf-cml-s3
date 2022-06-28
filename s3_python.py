
import cml.data_v1 as cmldata
from pyspark.sql import SparkSession

# Then call the `getOrCreate()` method of
# `SparkSession.builder` to start a Spark application.
# This example also gives the Spark application a name:
CONNECTION_NAME = "acc-rutgers"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
# Now you can use the `SparkSession` named `spark` to read
# data into Spark.
# ## Reading Data
# Read the flights dataset. This data is in CSV format
# and includes a header row. Spark can infer the schema
# automatically from the data:
flights = spark.read.csv('s3a://pjmf-ruora-bucket/pjmf-ruora-data/s3-test/flights.csv', header=True, inferSchema=True)

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
