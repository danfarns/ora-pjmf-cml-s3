import constants
# constants.TYPE: Output type. Please see constants file for available output types

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