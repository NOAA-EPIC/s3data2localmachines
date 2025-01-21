# s3data2localmachines
The purpose of this project is copy/download NOAA FIX data on s3:
https://noaa-nws-global-pds.s3.amazonaws.com/index.html
to local machines, like AWS cluster/instance

how to use "aws s3 cp" to copy data from aws s3 site: https://noaa-nws-global-pds.s3.amazonaws.com/
To copy data from the AWS S3 bucket noaa-nws-global-pds using the aws s3 cp command, you can follow these steps:

1. Install and Configure AWS CLI
If you haven't already, install the AWS CLI on your machine. You can download it from here.

Configure the AWS CLI with your credentials by running:

bash
Copy
aws configure
You'll need to provide your AWS Access Key ID, Secret Access Key, default region, and output format.

2. Use the aws s3 cp Command
The basic syntax for copying data from an S3 bucket is:

bash
Copy
aws s3 cp s3://<bucket-name>/<path-to-file> <local-destination>
For example, if you want to copy a file named example.txt from the noaa-nws-global-pds bucket to your local directory, you would use:

bash
Copy
aws s3 cp s3://noaa-nws-global-pds/example.txt ./example.txt
3. Copying a Directory
If you want to copy an entire directory, you can use the --recursive flag:

bash
Copy
aws s3 cp s3://noaa-nws-global-pds/path/to/directory ./local-directory --recursive
4. Example: Copying a Specific File
Suppose you want to copy a file located at s3://noaa-nws-global-pds/data/example.csv to your current directory:

bash
Copy
aws s3 cp s3://noaa-nws-global-pds/data/example.csv ./example.csv
5. Example: Copying All Files in a Directory
If you want to copy all files from a specific directory in the bucket to a local directory:

bash
Copy
aws s3 cp s3://noaa-nws-global-pds/data/ ./local-data/ --recursive
6. Additional Options
Dry Run: If you want to see what files would be copied without actually copying them, use the --dryrun option:

bash
Copy
aws s3 cp s3://noaa-nws-global-pds/data/ ./local-data/ --recursive --dryrun
Exclude/Include: You can use --exclude and --include to filter files:

bash
Copy
aws s3 cp s3://noaa-nws-global-pds/data/ ./local-data/ --recursive --exclude "*.log" --include "*.csv"
7. Check the S3 Bucket Structure
If you're unsure of the exact path, you can list the contents of the bucket using:

bash
Copy
aws s3 ls s3://noaa-nws-global-pds/ --recursive
8. Copying from a Public Bucket
Since noaa-nws-global-pds is a public bucket, you don't need AWS credentials to access it. However, you still need to use the aws s3 cp command as shown above.

Example Command:
bash
Copy
aws s3 cp s3://noaa-nws-global-pds/data/example.csv ./example.csv
This command will copy the example.csv file from the noaa-nws-global-pds bucket to your current directory.

Note:
Ensure you have the necessary permissions to access the S3 bucket and its contents.

If the bucket is public, you can access it without credentials, but you still need to use the AWS CLI commands as described.

This should help you copy data from the noaa-nws-global-pds S3 bucket to your local machine or another S3 location.


[Wei.Huang@awsepicweirocky8c7i48xlarge-25 fix]$ aws configure
AWS Access Key ID [None]: AWSdummyaccesskey
AWS Secret Access Key [None]: thereisnone
Default region name [None]: us-east-1
Default output format [None]: json
[Wei.Huang@awsepicweirocky8c7i48xlarge-25 fix]$ aws s3 ls s3://noaa-nws-global-pds

An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied
[Wei.Huang@awsepicweirocky8c7i48xlarge-25 fix]$ aws s3 ls s3://noaa-nws-global-pds/

An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied


