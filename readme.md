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

# Checking Permissions
## AWS Permission for File Uploading

## CDP for File Access Privileges

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



# Setting up your Project

## Starting up Spark 
* Unless you have a specific reason to use the older Spark version, you should choose the most recent spark.
  * At the time of this writing, it is `3.2.0`. Take note of this version number.

## Reading from S3 Buckets
To make the most of this section, you should have data you would like uploaded into your Amazon AWS S3 Bucket.
### Python Specific Instructions
...
* You will get a new tab on your interface "Spark UI" while you are connected.
   * This will go away when you disconnect
### R Specific Instructions
...
* You will get a new tab on your interface "Spark UI"
   * This will go away when you disconnect via `spark_disconnect(...)`
### Troubleshooting / Errors   


## Writing To S3 Buckets
NYI
### Python Specific Instructions
NYI
### R Specific Instructions
NYI
### Troubleshooting / Errors   
NYI