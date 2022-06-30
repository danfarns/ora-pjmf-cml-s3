# ORA/PJMF Cloudera Machine Learning - Reading from your Amazon S3 Bucket into Spark (pyspark or sparklyr)
This repository is used to store sample code in Python and R on how to connect to your S3 Amazon bucket from within your Cloudera Machine Learning environment with example files and setup instructions to make it easier for you to get started completely in the cloud.

# Guide Information / Sources 
* https://github.com/cloudera/cml-training
  * This repository contains all of the sample code to run the full suite of tutorials
* https://docs.cloudera.com/runtime/7.2.14/developing-spark-applications/topics/spark-s3.html
* https://spark.rstudio.com/guides/connections.html
* Abbreviations / Short Names
  * **Cloudera Data Platform**: CDP
  * **Machine Learning Workspace**: ML Workspace

# Requirements
* (Reword or Clarify Statement) The ability to add the correct permissions or receive the correct permissions from your administrator
  * (Get Picture|Clarify Statement) Environment -> Manage Access -> IDBroker Mappings (tab) -> Edit (Current Mappings) -> Add ur username (<ur_user>) -> and use the same role as “Data Access Role”
     * SELF NOTE: How to check if the current permissions are sufficient????
* (This being the runtimes) Either the Python Environment Runtimes or R Envionment Runtimes

# CDP On-Boarding
((?? Do I need this section ??))

# Checking Permissions
((?? What permissions do I need to check for ??))
## AWS Permission for File Uploading
((?? What should happen if I don't have permissions? I assume I just can't see this page or be able to upload anything; or the upload button will be disabled ??))

## CDP for File Access Privileges
((?? What settings in CDP may need to be set? 
```
so go to ur environment -> Manage Access -> IDBroker Mappings (tab) -> Edit (Current Mappings) -> Add ur username (<ur_user>) -> and use the same role as “Data Access Role” (edited) 

im going to create a doc today in the KB section of Confluence that u all have access to, since this seems to be a regular thing (of course it is)
```
??))

# Adding Files to your S3 Bucket
You will need to be logged into your Amazon AWS Account to follow these instructions. If you do not know the URL to log in, please ask your administrator for help.

1. Navigate to your S3 Dashboard
  - If you are having trouble, you can press <kbd>Alt</kbd>+<kbd>S</kbd> or click on the search bar at the top and type `S3`.
  - If you have followed along with Cloudera's On-boarding guide, you should already have buckets made that will allow Cloudera to have easier access.
    - In this guide, our bucket is named `pjmf-ruora-bucket`. You will need to follow along using your CDP accessible bucket name or ask your administrator for the appropriate bucket to place data for CDP in.
1. You should be looking at your S3 Buckets, if not click on "Buckets" in the left side panel near the top.
1. Click the name of the bucket you want to put your data into for CDP to access.
1. From here, your screen should now be similar to the following: ((PICTURE HERE: s3-bucket-helper-image-1))
1. (Optional) Create a new folder to store your data in.
  - While this is an optional step, this is a good practice to keep your data orderly as well as minimize interfering with existing data and other collaborators.
  - In this guide, the folder's name will be `s3-data-folder`
  - During the creation process, you may be asked if you would like Server-side encryption and other options. Ask your administrator for help if you are handling sensitive data as they should be able to guide you through these steps.
  - After you have clicked "Create folder", it should appear on your list.
  - Click on the new folder name you have just created to enter into it.
  - You can repeat this step as many times as necessary
1. When you have opened to the folder you want to store your S3 data in, click either of the "Upload" buttons.
1. You can add files or folders into this bucket.
  - If you are just testing your Cloudera Machine Learning Workspace and want to simply check connections, feel free to use the [flights.csv](data/flights.csv) provided.
  - You may set additional permissions and / or properties for these files if you choose. This guide will use the default selections for those sections.
1. When you have added all the files you want to upload to your s3 bucket, press the "Upload" button at the bottom.
1. If any errors occur, you will need to solve them or ask your administrator for help.
1. Otherwise you should see an "Upload succeeded" message with details. You may click the "Close" button near the upper right hand side.
1. After clicking "Close" your file should now appear in the list.
  - We will need to reference this folder and files, so keep a note where these files are or how to navigate back to these files through the web browser as it will be important in later sections of this guide.
1. If you are following this guide, to get the S3 URI for your [flights.csv](data/flights.csv) file that you just uploaded, click on "flights.csv" in the list.
1. On the right hand side should be an "S3 URI" link (`s3://your-bucket-name-here/.../flights.csv`). Copy this link somewhere or remember how to get back to this, since we will need it in a later section.


# Provisioning / Starting Your ML Workspace
You will need to be logged into your CDP from this point on as well as have your Amazon AWS Environment available. To check the availability of your Environments, you will need to navigate to your Cloudera Management Console > Environments tab or ask your administrator for help. If your environment is off and you have access, you will need to turn the environment on by clicking on the checkbox and clicking "Start Environment". This may take a while, so when it is finished, continue on this section.

1. Navigate to your Cloudera Machine Learning Dashboard.
  - This will be a page that will show all of your existing Machine Learning Workspaces if any are available.
1. Click the button in the upper right corner that says "Provision Workspace".
1. You should be looking at a screen that is asking for a Workspace Name and the Environment.
1. Give your workspace a valid unique name.
  - This guide will use the workspace name `s3-intro`
1. Select the environment to run this ML Workspace on.
1. Toggle advanced options on.
  1. For your instance type, you will need to choose what is appropriate for your needs. This guide will use `m5.2xlarge` so I can run two sessions.
  1. (Optional) This guide will NOT be using GPU Instances, so we will turn GPU Instances to "OFF".
    - If you will need GPU Instances for when you are doing your data, feel free to keep it in.
  1. (Optional) For our purposes, our group needs to check "Enable Public IP Address for Load Balancer".
    - If you do not require this option, feel free to leave it unchecked.
  1. (Optional) If your group needs other specific options filled out for your Workspace, feel free to enter them in.
  1. When you are ready to start your ML Workspace, press the "Provision Workspace" button at the bottom of your screen.
1. This process may take a while to get started. When it is started and its status is "Ready", move on to the 



# Setting Up Your ML Workspace
If this is a new ML Workspace, then there will be no other projects already set up.  
If you are following this guide exactly, we will be pulling this GitHub repository in and using the `s3_flight` scripts that will be using the [flights.csv](data/flights.csv) provided.

## Create a New Project
1. Click on "New Project" or in the upper right corner, press the "+" button and click "Create Project" from the dropdown menu.
1. You will now be on a screen that is asking for project Information.
1. Enter a project name.
  - This guide will use the project name `s3-spark`
1. (Optional) Enter a description if you want to.
1. (Optional) Select your project's visibility.
  - This guide wil stay on Private visibility
1. Under Initial Setup, click on "Git"
1. Enter this GitHub's Repo into the "Git URL of Project" text box, which should be "https://github.com/danfarns/ora-pjmf-cml-s3"
1. Decide which programming language you are most likely going to be using. 
  - If you know which programming language you want to use, feel free to stay on Basic and choose the appropriate Kernel for your project.
    * This guide was tested on the following Runtimes, but higher versions of these runtimes should work as well:
      - `Workbench - Python 3.7 - Standard - 2022.04` for Python
      - `Workbench - R 4.1 - Standard - 2022.04` for R
  - If you want to follow this guide or if you want to use both in one project, here are the steps for adding both runtimes I used to the project
    1. Click on Advanced
    1. Select the following options from the dropdown and press "Add Runtime":
      1. Workbench - Python 3.7 - Standard
      1. Workbench - R 4.1 - Standard
    1. Your Runtime setup should look like this if you are following the guide exactly: ((PICTURE HERE: s3-new-project-runtimes-1))
1. Press "Create Project"
  - The ML Workspace will automatically pull this repository's code into a project for you.
1. There should now be a "New Session" button near the top right. If there is not a "New Session" button, you will need to navigate to the new project's dashboard or click "Sessions" on the left side menu.


## Starting up Spark in Your Session
Unless you have a specific reason to use the older Spark version, you should choose the most recent spark.
* At the time of this writing, it is `3.2.0`. Take note of this version number.

1. If you have not already, create a new session from your ML Workspace by either clicking on "New Session" or navigating to the "Sessions" of your project's dashboard and clicking on "New Session"
1. You should now be looking at a "Start A New Session" Screen with the GitHub repository pulled in if you are following this guide.
1. Choose a session name
  - I will use the session name `s3-spark-python` for the Python version and `s3-spark-R` for the R version.
1. You will need to choose which language you want to use.
  - If you are using Python, select or use the [Editor: Workbench, Kernel: Python 3.x] from the dropdown(s), if it is available.
  - If you are using R, select or use the [Editor: Workbench, Kernel: R 4.1] from the dropdown(s), if it is available.
1. There should be a switch that says "Enable Spark".
  - If you do not have this switch or it is disabled, you will need to ask your administrator for help on enabling your ability to use Spark.
  - Take note of the latest version number. We may need it for another section.
1. If you have a dropdown, choose the latest version of Spark you can unless you have a reason to stick with an older version.
1. When you are ready, click "Start Session" in the lower right hand corner.
1. A popup should appear that is title "Connection Code Snippet". Do not close this window yet.

## Reading from S3 Buckets
To make the most of this section, you should have data you would like uploaded into your Amazon AWS S3 Bucket. If you are following this guide, you should have uploaded [flights.csv](data/flights.csv) and know the URI to it. Refer to the `Adding Files to your S3 Bucket` section to find the URI to your flights.csv file.

* While you are connected to spark, you may notice a new tab called "Spark UI" popup in the right side panel next to the "Session" and "Logs" tab. This will go away when your code hits a R `spark_disconnect(spark)` statement or a Python `spark.stop()` statement.

### Python Specific Instructions
If you are reading s3 data using R, please go to the next "R Specific Instructions" section.

1. If you accidently closed the popup that was title "Connection Code Snippet" or it went away, there should be a "Data" button on the top menu bar to the right. Clicking that will bring the window back up.
1. Make a note of the variable `CONNECTION_NAME` or copy that line. We will use this in the `s3_flights_python.py` file.
1. Now you can close the window by clicking "Close".
1. On the left most side of your screen should be a small list of files. Among these files should be a file called `s3_flights_python.py`
1. Click on `s3_flights_python.py`. This should open the editor window to s3_flights_python.py. 
1. Near the top of the file are two vairables:
  - Line 7: `CONNECTION_NAME`
  - Line 8: `S3A_AMAZON_BUCKET_FILE`
  - Fill in `CONNECTION_NAME` with the string given to you in step 2 of this section.
  - Fill in `S3A_AMAZON_BUCKET_FILE` with the S3 URI for your data.
1. PySpark cannot read from an "s3://" protocol directly, it must either be an "s3a://" or "s3n://".
  - This guide has only tried "s3a://". If you need to access via "s3n://", this guide may still work but might require additional steps that you will need to ask your administrator for help.
  - Make a modification to your `S3A_AMAZON_BUCKET_FILE` string by adding an "a" after "3". The string should now be an s3a:// protocol.
  - For example, if your S3 URI was `s3://pjmf-ruora-bucket/s3-data-folder/flights.csv`, it should now read  `s3a://pjmf-ruora-bucket/s3-data-folder/flights.csv`
1. After you have updated these two variables, press "Run" at the top
  - And then click "Run All"

If all is setup, and you have the correct permissions previously set up, this should run the tutorial code snippet found at https://github.com/cloudera/cml-training/blob/master/sessions/pyspark.py and it should disconnect from spark at the end during Cleanup.

Congratulations, you have now read from a s3 storage system and used PySpark! 

### Python Troubleshooting
1. Error: `Exception: Please update the variable [CONNECTION_NAME] with the connection name you were given.`
  - You need to fill out the `CONNECTION_NAME` variable.
1. Error: `ValueError: No data connection named ---- found`
  - Click the "Data" button on the top right menu and make sure the CONNECTION_NAME matches the CONNECTION_NAME in the popup window.
1. Error: `org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "s3"`
  - Your `S3A_AMAZON_BUCKET_FILE` variable needs to start with "s3a" or "s3n". We suggest "s3a" first.
1. Error: `java.nio.file.AccessDeniedException: _YOUR_S3A_BUCKET_FILE: org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: There is no mapped role for the group(s) associated with the authenticated user. (user: YOUR_USER)`
  - There is a problem with your permissions and you will need to be added to the role. ((Hazem input needed; will refer to an above section?))
  - You will need the "Data Access Role" associated with your user.

If you run into other problems and come across fixes for them, please leave me a note in the form of an issue and I will add it to the list.

### R Specific Instructions
If you are reading s3 data using Python, please go to the previous "Python Specific Instructions" section.

1. We don't need this Connection window, so feel free to close it using the "Close" button in the lower right of the modal popup.
1. After you session has initialized, we need to install `sparklyr`. 
  - Run the command in the right side window: `install.packages('sparklyr')`
  - This command will take a while to run. You can continue the next steps while it is installing.
1. On the left most side of your screen should be a small list of files. Among these files should be a file called `s3_flights_R.R`
1. We need to set a few variables up:
  1. Line 29: **spark_version** - This should be set to the version of spark that you are using (please refer to a previous section). If you leave this blank, the program may automatically figure it what version to use, but this guide will define it here.
  1. Line 30: **s3a_amazon_bucket** - This is the bucket without the trailing slash. If you have multiple buckets, you can separate them by commas.
  1. Line 31: **s3a_amazon_bucket_file** - This is the direct link to your S3 bucket file.
1. sparklyr cannot read from an "s3://" protocol directly, it must either be an "s3a://" or "s3n://".
  - This guide has only tried "s3a://". If you need to access via "s3n://", this guide may still work but might require additional steps that you will need to ask your administrator for help.
  - Make a modification to your `s3a_amazon_bucket` and `s3a_amazon_bucket_file` string by adding an "a" after "3". The string should now be an s3a:// protocol.
  - For example, if your S3 URI was `s3://pjmf-ruora-bucket/s3-data-folder/flights.csv`, it should now read  `s3a://pjmf-ruora-bucket/s3-data-folder/flights.csv`
1. After you have updated these two variables, and sparklyr has finished installing, press "Run" at the top
  - And then click "Run All"

If all is setup, and you have the correct permissions previously set up, this should run the tutorial code snippet found at https://github.com/cloudera/cml-training/blob/master/sessions/sparklyr.R and it should disconnect from spark at the end during Cleanup.

Congratulations, you have now read from a s3 storage system and used sparklyr! 

### R Troubleshooting
1. Error: `Please enter your Amazon Bucket URI here`
  - You need to fill in your `s3a_amazon_bucket` variable.
1. Error: `Please enter your S3A File URI Here`
  - You need to fill in your `s3a_amazon_bucket_file` variable.
1. Error: `org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "s3"`
  - Your `S3A_AMAZON_BUCKET_FILE` variable needs to start with "s3a" or "s3n". We suggest "s3a" first.
1. Error `org.apache.spark.sql.AnalysisException: Path does not exist` or `org.apache.spark.SparkException: Job aborted due to stage failure`
  - R may not fail on a bad bucket name, but will disrupt your access. Double check and make sure your `s3a_amazon_bucket` and `s3a_amazon_bucket_file` are correct and with the s3a protocol designation.
  - You may have to disconnect from the R session by manually entering `spark_disconnect(spark)` into the console on the right and running the script from the start again since R will use the already opened Spark Session to continue from if you fixed a typo.
1. Error: `java.nio.file.AccessDeniedException: _YOUR_S3A_BUCKET_FILE: org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: There is no mapped role for the group(s) associated with the authenticated user. (user: YOUR_USER)`
1. Error: `java.lang.IllegalStateException: Authentication with IDBroker failed.`
  - There is a problem with your "conf" variable that needs to be addressed or added to. 
    - You need to add your s3a_amazon bucket to the `conf$spark.yarn.access.hadoopFileSystems` variable.
    - If you are using multiple buckets, this is a string separated by a comma.
      - e.g. `s3a://bucket-1,s3a://bucket-2` (as per https://docs.cloudera.com/runtime/7.2.14/developing-spark-applications/topics/spark-s3.html)
  - You will need to contact your administrator if you need other specific variables set

If you run into other problems and come across fixes for them, please leave me a note in the form of an issue and I will add it to the list.



## Writing To S3 Buckets
NYI
### Python Specific Instructions
NYI
### R Specific Instructions
NYI
### Troubleshooting / Errors   
NYI