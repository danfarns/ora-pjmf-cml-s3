import sys
import s3_python_module
import constants


s3_python_module.printOut("If you are seeing this message, the module has imported successfully.", False)
s3_python_module.InitializeAwsS3EnvironmentVariables()
#s3_python_module.TestConnection_Find_Buckets()
#s3_python_module.TestConnection_FindPythonFlightsCsv()

"""
Configuration for downloading a single file

"""
OUTPUT_INFORMATION = s3_python_module.GetOutputInformation()


#s3_python_module.TestConnection_FindPythonFlightsCsv2(BUCKET,DIRECTORY_PREFIX)

if OUTPUT_INFORMATION.AUTOMATIC_OUTPUT:
	Output_Success=False

	if OUTPUT_INFORMATION.OUTPUT_CONFIG.TYPE == constants.TYPE.SINGLE_FILE_CSV:
		Output_Success=s3_python_module.OutputAsSingleCSV(
			OUTPUT_INFORMATION.OUTPUT_CONFIG.S3_BUCKET_NAME, 
			OUTPUT_INFORMATION.OUTPUT_CONFIG.S3_KEY_PREFIX,
			OUTPUT_INFORMATION.OUTPUT_CONFIG.OUTPUT_DIRECTORY,
			OUTPUT_INFORMATION.OUTPUT_CONFIG.OUTPUT_FILENAME,
			OUTPUT_INFORMATION.OUTPUT_CONFIG.OTHER_OPTIONS
		)
	else:
		s3_python_module.printOut("Unknown Automatic output type.")
		sys.exit("Please check [constants.py] for available output types or type may not be implemented yet for automatic output.")
	

	if not Output_Success:
		sys.exit("The output has not processed successfully. Please check the console output for more information.")
	else:
		s3_python_module.printOut("The output has processed successfully.")
	sys.exit(0)

else:
	##WRITE YOUR SCRIPT HERE IF YOU ARE NOT USING THE AUTOMATIC OUTPUT FEATURE


	pass

