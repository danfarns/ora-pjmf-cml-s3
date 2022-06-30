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
# Read the table dataset. This data is in CSV format
# and includes a header row. Spark can infer the schema
# automatically from the data:


spark_table = spark.read.csv(S3A_AMAZON_BUCKET_FILE, header=True, inferSchema=True)

# The result is a Spark DataFrame named `spark_table`.
# ## Inspecting Data
# Inspect the DataFrame to gain a basic understanding
# of its structure and contents.
# Print the number of rows:
spark_table.count()
# Print the schema:
spark_table.printSchema()
# Print five rows:
spark_table.limit(5).show()
# Or more concisely:
spark_table.show(5)
# Print 20 rows (the default number is 20):
spark_table.show()


# ## Cleanup

# Stop the Spark application:

spark.stop()