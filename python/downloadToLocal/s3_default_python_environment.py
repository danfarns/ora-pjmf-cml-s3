import constants
# constants.TYPE: Output type. Please see constants file for available output types
# To create your own environment, copy this file to `s3_python_environment.py`. 
# Make sure to add the `s3_python_environment.py` file to your .gitignore if you haven't already
#NOTICE: If you are running specifically from ArcPro via the toolbox, this and `s3_python_environment` will be ignored.

AWS_CONNECTION_INFO = {
	'AWS_ACCESS_KEY_ID': "", 
	'AWS_SECRET_ACCESS_KEY': "",

	'REGION': 'us-east-1'
}
OUTPUT_INFORMATION={
	'AUTOMATIC_OUTPUT': False,
	#OUTPUT_CONFIG is only necessary if AUTOMATIC_OUTPUT is set to True
	'OUTPUT_CONFIG': {
		'TYPE':None, #Must be of Type.* constant from constants.py
		'S3_BUCKET_NAME':None,
		'S3_KEY_PREFIX':None,
		#If on windows, you may want to consider putting `r` in front of the string
		'OUTPUT_DIRECTORY':None,
		'OUTPUT_FILENAME':None,
		'OTHER_OPTIONS':None
	}
}