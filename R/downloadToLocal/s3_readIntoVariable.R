#importing the library
library("aws.s3")


# Variables used for configuration
s3_amazon_bucket <- ""
s3_amazon_bucket_prefix <- ""

#Variable to read into the global environment (at least for now)
#you can use YOUR_GOOD_VAR_NAME <- get(csvVariableName) to pull the data into a variable of your choosing.
csvVariableName <- "combinedCsv"
dataHasHeaders <- TRUE



Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-east-1"
)




readFromS3IntoDataFrame <- function(filename, bucket, sep = ",", dataHasHeaders = TRUE){
  
  if( exists(csvVariableName) == TRUE ) {
    assign(csvVariableName, 
           rbind( get(csvVariableName) ,s3read_using(FUN=read.csv, bucket = bucket, object=filename,sep = sep, header=dataHasHeaders) ),
           envir = globalenv()
    )
  }else{
    assign(csvVariableName, s3read_using(FUN=read.csv, bucket = bucket, object=filename,sep = sep, header=dataHasHeaders),
           envir = globalenv() )
  }
  
  
  return(TRUE)
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
  
  #print( paste0("Csv: ", bucket_info$Key) ) 
  readFromS3IntoDataFrame(bucket_info$Key, bucket_info$Bucket, sep=',', dataHasHeaders=dataHasHeaders) 
}




myCsvDataframe = get(csvVariableName)
print(myCsvDataframe)

#x= readFromS3('pjmf-ruora-data/s3-data-folder/flights.csv_output_python/', this_bucket, sep='\t')



