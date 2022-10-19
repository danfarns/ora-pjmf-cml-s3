#importing the library
library("aws.s3")


# Variables used for configuration
s3_amazon_bucket <- ""
s3_amazon_bucket_prefix <- ""



Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-east-1"
)



readFromS3IntoDataFrame <- function(filename, bucket, sep = ","){
  return(s3read_using(FUN=read.csv, bucket = bucket, object=filename,sep = sep, header=T))
}


this_bucket = get_bucket(s3_amazon_bucket, prefix=s3_amazon_bucket_prefix)


#Loop over bucket
for(bucket_info in this_bucket) {
  
  
  if( is.null(bucket_info$Key) ) {
    next;
  }
  if( endsWith(bucket_info$Key, '.csv') == FALSE ) {
    next;
  }
  
  print( paste0("Csv: ", bucket_info$Key) ) 
  x = readFromS3(bucket_info$Key, bucket_info$Bucket, sep=',') 
  print(x)
}


#x= readFromS3('pjmf-ruora-data/s3-data-folder/flights.csv_output_python/', this_bucket, sep='\t')



