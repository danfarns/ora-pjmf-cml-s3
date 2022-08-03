# -*- coding: utf-8 -*-

import arcpy
import os
import tempfile

from sqlalchemy import true
import s3_python_module

#https://pro.arcgis.com/en/pro-app/latest/arcpy/geoprocessing_and_python/defining-parameters-in-a-python-toolbox.htm
#https://pro.arcgis.com/en/pro-app/latest/arcpy/geoprocessing_and_python/defining-parameter-data-types-in-a-python-toolbox.htm

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = "toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [S3ToArcTable]


class S3ToArcTable(object):
    def __init__(self):
        """S3 to Arc Table allows a user to download directly from ArcPro."""
        self.label = "S3 To ArcGIS Data Table"
        self.description = "Allows a user to grab data from their Amazon S3 bucket and add it to the current ArcPro map. This will use a temporary directory to download a single CSV to and place it into your selected output area."
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        #Workspace
        param0 = arcpy.Parameter(
            displayName="Output Table Location",
            name="output_table_location",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        #Default values
        param0.defaultEnvironmentName = "workspace"
            
        param1 = arcpy.Parameter(
            displayName="Output Table Name",
            name="output_table_name",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param2 = arcpy.Parameter(
            displayName="Amazon S3 Access Key ID",
            name="s3_access_key_id",
            datatype="String",
            parameterType="Required",
            direction="Input")
            
        param3 = arcpy.Parameter(
            displayName="Amazon S3 Secret Access Key",
            name="s3_secret_access_key",
            datatype="String",
            parameterType="Required",
            direction="Input")
            
        param4 = arcpy.Parameter(
            displayName="Bucket Name",
            name="s3_bucket_name",
            datatype="String",
            parameterType="Required",
            direction="Input")
        param5 = arcpy.Parameter(
            displayName="S3 Folder Location",
            name="s3_folder_location",
            datatype="String",
            parameterType="Required",
            direction="Input")

            
        param6 = arcpy.Parameter(
            displayName="My CSV(s) Has Headers",
            name="csv_has_headers",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        param6.value=False

        param7 = arcpy.Parameter(
            displayName="Enforce Same Headers Check",
            name="enforce_same_headers_check",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        param7.value=True

        params = [param0,param1,param2,param3,param4,param5,param6,param7]

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        #arcpy.ClearWorkspaceCache_management()
        #arcpy.AddMessage("Test 1")
        #s3_python_module.printOut("Testing execution!", True)
        #arcpy.AddMessage("Test 2")
        WORKSPACE = parameters[0].valueAsText
        TABLE_EXPORT_NAME = parameters[1].valueAsText

        AWS_ACCESS_KEY_ID = parameters[2].valueAsText
        AWS_SECRET_ACCESS_KEY = parameters[3].valueAsText
        
        S3_BUCKET = parameters[4].valueAsText
        S3_KEY_PREFIX = parameters[5].valueAsText
        hasHeader=parameters[6].value
        checkSameHeader=parameters[7].value

        DESCRIBE_WORKSPACE = arcpy.Describe(WORKSPACE)

        with tempfile.TemporaryDirectory() as TEMP_DIRECTORY:
            TEMP_FILENAME="output"
            s3_python_module.InitializeAwsS3EnvironmentVariablesManually(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY, True)
            s3_python_module.OutputAsSingleCSV(S3_BUCKET, S3_KEY_PREFIX, TEMP_DIRECTORY, TEMP_FILENAME, {
                'hasHeader': hasHeader,
                'checkSameHeader':checkSameHeader,
                'printToArcpy': True
            })

            if DESCRIBE_WORKSPACE.workspaceType == 'FileSystem':
                TABLE_EXPORT_NAME = "{0}.{1}".format(TABLE_EXPORT_NAME, "dbf")

            ExportLocation = os.path.join(WORKSPACE,TABLE_EXPORT_NAME) 
            ImportLocation = os.path.join(TEMP_DIRECTORY, "{0}.csv".format(TEMP_FILENAME))

            s3_python_module.printOut("Exporting [{0}] to [{1}]".format(
                ImportLocation, 
                ExportLocation
            ))
            arcpy.conversion.ExportTable(ImportLocation, ExportLocation)
            #Try adding the table to the map
            try:
                CURRENT_MAP = arcpy.mp.ArcGISProject("CURRENT")
                NEW_TABLE = arcpy.mp.Table(ExportLocation)
                mapList = CURRENT_MAP.listMaps()[0]
                mapList.addTable(NEW_TABLE)
                    
            except RuntimeError:

                pass






        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
