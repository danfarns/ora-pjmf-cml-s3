# Copyright 2020 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# # sparklyr Example

# The code in this file requires the sparklyr package.
# If this code fails to run in an R session, install
# sparklyr by running `install.packages("sparklyr")`

# ## Starting a Spark Application

# Begin by loading the sparklyr package:

#install.packages("sparklyr")
library(sparklyr)

# Variables used for configuration
spark_app_name <- "sparklyr-app"
spark_version <- "3.2.0"
s3a_amazon_bucket <- ""
s3a_amazon_bucket_file <- ""
spark_table_name <- "spark_table"

if (s3a_amazon_bucket == "") {
 stop("Please enter your Amazon Bucket URI here");
}

if (s3a_amazon_bucket_file == "") {
 stop("Please enter your S3A File URI Here");
}



# Then call `spark_connect()` to start a Spark application.
# This example also gives the Spark application a name:
conf <- spark_config()

conf$spark.yarn.access.hadoopFileSystems<-s3a_amazon_bucket

spark <- spark_connect(app_name = spark_app_name, version=spark_version, master="yarn-client", config=conf)

# Now you can use the connection object named `spark` to
# read data into Spark.


# ## Reading Data

# Read the table dataset. This data is in CSV format
# and includes a header row. Spark can infer the schema
# automatically from the data:

spark_table <- spark_read_csv(
  sc = spark,
  name = spark_table_name,
  path = s3a_amazon_bucket_file,
  header = TRUE,
  infer_schema = TRUE
)

# The result is a Spark DataFrame named `spark_table`. Note
# that this is not an R data frame—it is a pointer to a
# Spark DataFrame.

spark_write_csv(spark_table, paste0(s3a_amazon_bucket_file, "_output_R"))
#spark_write_csv(spark_table, paste0(s3a_amazon_bucket_file, "_output_R"), mode="overwrite")

# ## Cleanup

# Stop the Spark application:

spark_disconnect(spark)
