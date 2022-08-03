"""
#
# References:
#   https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-in-amazon-s3-buckets.html
"""

#General import statements here.
import s3_default_python_environment
__USER_ENVIRONMENT_IMPORTED = False
__ARCPY_IMPORTED = False
S3_READY_FOR_USE = False

try:
	import arcpy
	__ARCPY_IMPORTED = True
except ImportError:
	print("ArcPY was not found so it was not imported")
	pass

try:
	import s3_python_environment
	__USER_ENVIRONMENT_IMPORTED = True
except ImportError:
	print("Did not find a user created `s3_python_environment.py` file. You should probably have one instead of using the s3_default_python_environment if you are uploading code to GitHub/GitLab or other version control services.")
	pass

import os
import csv
import boto3 #pip3 install boto3

class OutputConfig:
	TYPE=None
	S3_BUCKET_NAME=None
	S3_KEY_PREFIX=None
	OUTPUT_DIRECTORY=None
	OUTPUT_FILENAME=None
	OTHER_OPTIONS=None
	
	def ExtractOutputConfigVariables(self, OUTPUT_CONFIG):
		self.TYPE=OUTPUT_CONFIG['TYPE']
		self.S3_BUCKET_NAME=OUTPUT_CONFIG['S3_BUCKET_NAME']
		self.S3_KEY_PREFIX=OUTPUT_CONFIG['S3_KEY_PREFIX']
		#Trying to keep the slashes the same
		self.OUTPUT_DIRECTORY=os.path.abspath(os.path.join(OUTPUT_CONFIG['OUTPUT_DIRECTORY']))
		self.OUTPUT_FILENAME=OUTPUT_CONFIG['OUTPUT_FILENAME']
		self.OTHER_OPTIONS=OUTPUT_CONFIG['OTHER_OPTIONS']


class OutputInformation:
	AUTOMATIC_OUTPUT=False
	OUTPUT_CONFIG=OutputConfig()
	
	def ExtractEnvironmentVariables(self, OUTPUT_INFORMATION):
		self.AUTOMATIC_OUTPUT = OUTPUT_INFORMATION['AUTOMATIC_OUTPUT']

		if self.AUTOMATIC_OUTPUT:
			self.OUTPUT_CONFIG.ExtractOutputConfigVariables(OUTPUT_INFORMATION['OUTPUT_CONFIG'])


def InitializeAwsS3EnvironmentVariables():
	global S3_READY_FOR_USE

	if __USER_ENVIRONMENT_IMPORTED:
		printOut("Retrieving User Environment Variables.")
		for ENV_KEY in s3_python_environment.AWS_CONNECTION_INFO.keys():
			os.environ[ ENV_KEY ] = s3_python_environment.AWS_CONNECTION_INFO[ENV_KEY]
	else:
		printOut("Retrieving Default Environment Variables.")
		for ENV_KEY in s3_default_python_environment.AWS_CONNECTION_INFO.keys():
			os.environ[ ENV_KEY ] = s3_python_environment.AWS_CONNECTION_INFO[ENV_KEY]

	S3_READY_FOR_USE=True

	printOut("Initialized S3 Environment variables.")
	#printOut(os.environ)
	return True
def InitializeAwsS3EnvironmentVariablesManually(AWS_ACCESS_KEY_ID=None, AWS_SECRET_ACCESS_KEY=None, toArcpy=False):
	global S3_READY_FOR_USE
	os.environ[ 'AWS_ACCESS_KEY_ID' ]= AWS_ACCESS_KEY_ID
	os.environ[ 'AWS_SECRET_ACCESS_KEY' ]= AWS_SECRET_ACCESS_KEY
	printOut("Manually initialized S3 Environment variables.", toArcpy)

	S3_READY_FOR_USE=True
	return True	

def GetOutputInformation():
	OutputInformationObj = OutputInformation()
	if __USER_ENVIRONMENT_IMPORTED:
		OutputInformationObj.ExtractEnvironmentVariables(s3_python_environment.OUTPUT_INFORMATION)
	else:
		OutputInformationObj.ExtractEnvironmentVariables(s3_default_python_environment.OUTPUT_INFORMATION)

	return OutputInformationObj


def printOut(message="No message given.", toArcpy=False ):
	
	if toArcpy and __ARCPY_IMPORTED:
		arcpy.AddMessage(message)
	else:
		print(message)

# def requireArcPy():
# 	global __ARCPY_IMPORTED
# 	try:
# 		import arcpy
# 		__ARCPY_IMPORTED=True
# 		printOut("ArcPy Imported successfully.")
# 	except ImportError:
# 		printOut("ArcPy module was not found.")
# 		return False
	

# 	return True



####OUTPUT FUNCTIONS

def OutputAsSingleCSV(s3BucketName, s3KeyPrefix, localDirectory, outputName, options={'hasHeader':False, 'checkSameHeader':True, 'printToArcpy':False}):
	"""OutputAsSingleCSV
Attributes
----------
s3BucketName : str
	The name of the bucket your data is in
s3KeyPrefix : str
	An s3a Accessible directory/folder location that contains all of the CSVs you want to compile.
	If your full URI string is s3a://YOUR-BUCKET/Folder1/folder2/myData/, s3KeyPrefix would be "Folder1/folder2/myData/".
	This folder should contain a "_SUCCESS" typically and a bunch of part-xxxxx-UUID.csv(s)
localDirectory : str
	the local-to-your-computer location you want to combine your CSVs
outputName : str
	The name of the file. do not append ".csv", we'll do that.
options : dict
	Passing in "None" will use default values
	hasHeader: boolean
		default: false
		If your csvs have a header, they should all be the same header
	checkSameHeader: boolean
		default: true
		This option comes into affect if hasHeader is true. this will throw an error if a CSV that was downloaded has a different header than the previous files.
"""

####

	if options is None:
		options={'hasHeader':False, 'checkSameHeader':True, 'printToArcpy':False}
	
	
	hasHeader=False
	OutputHeader=None

	checkSameHeader=True
	
	printToArcpy=False

	TEMP_FILE = os.path.join(localDirectory, "tempfile.csv")

	if 'hasHeader' in options:
		hasHeader = options['hasHeader']
		if 'checkSameHeader' in options:
			checkSameHeader = options['checkSameHeader']

	if 'printToArcpy' in options:
		printToArcpy = options['printToArcpy']

	printOut("Downloading from S3 as a Single CSV file...", printToArcpy)

	if not S3_READY_FOR_USE:
		printOut("S3 Wasn't ready for use yet. Initializing Environment.", printToArcpy)
		InitializeAwsS3EnvironmentVariables()


	#Is local folder writable?
	with open(os.path.join(localDirectory, "{0}.csv".format(outputName)), 'wb') as touchedFile:
		if not touchedFile.writable():
			printOut("Local file location was not writable. Is this correct? [Directory: `{0}`, Filename: `{1}`]".format(localDirectory, "{0}.csv".format(outputName)), printToArcpy)
			return False
		

	#See if folder exists
	s3 = boto3.client('s3')

	CSV_List = s3.list_objects(Bucket = s3BucketName, Prefix=s3KeyPrefix)

	Csv_Success_Found=False
	Csv_Files = []

	for CSVFileObject in CSV_List.get('Contents'):
		#example of "o": {'Key': 's3-data-folder/flights.csv_output_python/part-00001-9e0062be-fea8-4343-b22e-66981b5a7091-c000.csv', 'LastModified': datetime.datetime(2022, 8, 2, 18, 7, 50, tzinfo=tzutc()), 'ETag': '"e9a3e06b71f4b160ff8885d4e67f03b4"', 'Size': 14006699, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'aws-root-0000144', 'ID': '05c7f77fb66cfaa5b23d1935c94e32d17ea317a5f4eeb794578ce723171b7e3c'}}
		#data = s3.get_object(Bucket=bucket, Key=o.get('Key'))
		#printOut(CSVFileObject)
		csvFilename = os.path.basename( CSVFileObject.get('Key') )
		#printOut(csvFilename)
		if csvFilename == '_SUCCESS':
			Csv_Success_Found=True
		elif csvFilename.endswith('.csv'):
			Csv_Files.append(CSVFileObject.get('Key'))
		else:
			printOut("> Unknown file: `{0}`".format( csvFilename ) , printToArcpy)
			continue

	if not Csv_Success_Found:
		printOut("Could not find a file named `_SUCCESS`. This file should appear if pyspark/sparklyr has output the data successfully. Ending program.", printToArcpy)
		return False
	
	if len(Csv_Files) == 0:
		printOut("Did not find any CSVs in this folder. Is this the correct folder? [s3a://{0}/{0}] Ending program.".format(s3BucketName, s3KeyPrefix), printToArcpy)
		return False


	

	printOut("Starting CSV Downloads...", printToArcpy)

	

	#Newline to empty is necessary so files get output properly.
	with open(os.path.join(localDirectory, "{0}.csv".format(outputName)), 'w', newline='') as singleCsvFile:
		myCsvWriter = csv.writer(singleCsvFile)

		for csvFileKey in Csv_Files:
			#start downloading files
			with open(TEMP_FILE, 'wb') as downloadingFile:
				s3.download_fileobj(s3BucketName,csvFileKey, downloadingFile)
			
			#New file is downloaded. start writing to the combined file.
			with open(TEMP_FILE, 'r') as downloadedFile:
				readHeader = False
				downloadedCsvReader = csv.reader(downloadedFile)
				for myDownloadedRow in downloadedCsvReader:
					if hasHeader and not readHeader:
						#Should only enter here on the first row of a new file if we have headers
						if OutputHeader is None:
							#New file, so new header
							OutputHeader=myDownloadedRow
							myCsvWriter.writerow(myDownloadedRow)
						elif checkSameHeader:
							if OutputHeader != myDownloadedRow:
								printOut("Headers did not align. Expected [{0}], Got: [{1}]".format(OutputHeader.join(','), myDownloadedRow.join(',')), printToArcpy)
								return False
						readHeader=True
					else:
						#just write the line to file. Not really a good way to check outside of counts. 
						#if you mix CSVs at this point, that's on you.
						myCsvWriter.writerow(myDownloadedRow)


	if os.path.exists(TEMP_FILE):
		os.remove(TEMP_FILE)

	printOut("Single CSV output should be located: [{0}].".format(os.path.join(localDirectory, "{0}.csv".format(outputName))), printToArcpy)

	return True

def OutputAsMultipleCSV(s3BucketName, s3KeyPrefix, localDirectory, outputName, options={'hasHeader':False, 'checkSameHeader':True}):
	"""OutputAsMultipleCSV
	
This function will simply throw an error if it detects a non-same header.

Attributes
----------
s3BucketName : str
	The name of the bucket your data is in
s3KeyPrefix : str
	An s3a Accessible directory/folder location that contains all of the CSVs you want to compile.
	If your full URI string is s3a://YOUR-BUCKET/Folder1/folder2/myData/, s3KeyPrefix would be "Folder1/folder2/myData/".
	This folder should contain a "_SUCCESS" typically and a bunch of part-xxxxx-UUID.csv(s)
localDirectory : str
	the local-to-your-computer location you want to combine your CSVs
outputName : str
	The name of the file. File output will be `{outputName}-{csvNumber}.csv`. Do not append ".csv", we'll take care of that.
options : dict
	Passing in "None" will use default values
	hasHeader: boolean
		default: false
		If your csvs have a header, they should all be the same header
	checkSameHeader: boolean
		default: true
		This option comes into affect if hasHeader is true. this will throw an error if a CSV that was downloaded has a different header than the previous files.

"""

####

	if options is None:
		options={'hasHeader':False, 'checkSameHeader':True}
	

	printOut("Outputting as multiple CSV files...")

	if not S3_READY_FOR_USE:
		printOut("S3 Wasn't ready for use yet. Initializing Environment.")
		InitializeAwsS3EnvironmentVariables()


	#Is local folder writable?
	with open(os.path.join(localDirectory, outputName), 'wb') as touchedFile:
		if not touchedFile.writable():
			printOut("Local file location was not writable. Is this correct? [Directory: `{0}`, Filename: `{1}`]".format(localDirectory, outputName))
			return False

	
	if os.path.exists(os.path.join(localDirectory, outputName)):
		os.remove(os.path.join(localDirectory, outputName))
		

	#See if folder exists
	s3 = boto3.client('s3')

	CSV_List = s3.list_objects(Bucket = s3BucketName, Prefix=s3KeyPrefix)

	Csv_Success_Found=False
	Csv_Files = []

	for CSVFileObject in CSV_List.get('Contents'):
		#example of "o": {'Key': 's3-data-folder/flights.csv_output_python/part-00001-9e0062be-fea8-4343-b22e-66981b5a7091-c000.csv', 'LastModified': datetime.datetime(2022, 8, 2, 18, 7, 50, tzinfo=tzutc()), 'ETag': '"e9a3e06b71f4b160ff8885d4e67f03b4"', 'Size': 14006699, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'aws-root-0000144', 'ID': '05c7f77fb66cfaa5b23d1935c94e32d17ea317a5f4eeb794578ce723171b7e3c'}}
		#data = s3.get_object(Bucket=bucket, Key=o.get('Key'))
		#printOut(CSVFileObject)
		csvFilename = os.path.basename( CSVFileObject.get('Key') )
		#printOut(csvFilename)
		if csvFilename == '_SUCCESS':
			Csv_Success_Found=True
		elif csvFilename.endswith('.csv'):
			Csv_Files.append(CSVFileObject.get('Key'))
		else:
			printOut("> Unknown file: `{0}`".format( csvFilename ) )
			continue

	if not Csv_Success_Found:
		printOut("Could not find a file named `_SUCCESS`. This file should appear if pyspark/sparklyr has output the data successfully. Ending program.")
		return False
	
	if len(Csv_Files) == 0:
		printOut("Did not find any CSVs in this folder. Is this the correct folder? [s3a://{0}/{0}] Ending program.".format(s3BucketName, s3KeyPrefix))
		return False


	

	printOut("Starting CSV Downloads...")

	hasHeader=False
	OutputHeader=None

	checkSameHeader=True

	if 'hasHeader' in options:
		hasHeader = options['hasHeader']


	for csvIdx, csvFileKey in enumerate(Csv_Files):
		DOWNLOAD_FILE_LOCATION = os.path.join(localDirectory, "{0}-{1}.csv".format(outputName, csvIdx) )
		#start downloading files
		with open(DOWNLOAD_FILE_LOCATION, 'wb') as downloadingFile:
			s3.download_fileobj(s3BucketName,csvFileKey, downloadingFile)
		
		#New file is downloaded. start writing to the combined file.
		with open(DOWNLOAD_FILE_LOCATION, 'r') as downloadedFile:
			downloadedCsvReader = csv.reader(downloadedFile)
			myDownloadedRow = next(downloadedCsvReader)
			if hasHeader:
				#Should only enter here on the first row of a new file if we have headers
				if OutputHeader is None:
					#New file, so new header
					OutputHeader=myDownloadedRow
				elif checkSameHeader:
					if OutputHeader != myDownloadedRow:
						printOut("(WARNING ONLY) Headers did not align with the first downloaded csv. File: [{0}], Expected [{1}], Got: [{2}]".format(DOWNLOAD_FILE_LOCATION, OutputHeader.join(','), myDownloadedRow.join(',')))

	return True
	
def InsertIntoArcGisTable(s3aBucketLocation, s3KeyPrefix, localDirectory, outputName):
	if not S3_READY_FOR_USE:
		InitializeAwsS3EnvironmentVariables()

	if not __ARCPY_IMPORTED:
		printOut("We could not import the arcpy module for ArcGIS processing. Please make sure you have arcpy installed or are using the correct Python Environment.")
		return False






	return True
	
def InsertIntoMySQLTable(s3aBucketLocation):
	if not S3_READY_FOR_USE:
		InitializeAwsS3EnvironmentVariables()
	return

def InsertIntoPostGRESQLTable(s3aBucketLocation, localDirectory, outputName):
	if not S3_READY_FOR_USE:
		InitializeAwsS3EnvironmentVariables()
	return


def TestConnection_Find_Buckets():
	if not S3_READY_FOR_USE:
		printOut("S3 Wasn't ready for use yet. Initializing Environment.")
		InitializeAwsS3EnvironmentVariables()
		
	s3 = boto3.resource('s3')

	printOut(s3)

	# Print out bucket names
	for bucket in s3.buckets.all():
		printOut(bucket.name)
	return
	
	
def TestConnection_FindPythonFlightsCsv2(bucket= '', prefix=''):
	printOut("Starting [TestConnection_FindPythonFlightsCsv]")
	if not S3_READY_FOR_USE:
		printOut("S3 Wasn't ready for use yet. Initializing Environment.")
		InitializeAwsS3EnvironmentVariables()
		

	s3 = boto3.client('s3')
	result = s3.list_objects(Bucket = bucket, Prefix=prefix)
	for o in result.get('Contents'):
		#example of "o": {'Key': 's3-data-folder/flights.csv_output_python/part-00001-9e0062be-fea8-4343-b22e-66981b5a7091-c000.csv', 'LastModified': datetime.datetime(2022, 8, 2, 18, 7, 50, tzinfo=tzutc()), 'ETag': '"e9a3e06b71f4b160ff8885d4e67f03b4"', 'Size': 14006699, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'aws-root-0000144', 'ID': '05c7f77fb66cfaa5b23d1935c94e32d17ea317a5f4eeb794578ce723171b7e3c'}}
		#data = s3.get_object(Bucket=bucket, Key=o.get('Key'))
		printOut(o)

	return

def initialize_module():
	InitializeAwsS3EnvironmentVariables()
	return


