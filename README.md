# Closure Reporting Bot - BC511
This bot is designed to look at the BC511/DriveBC public available API on a regular basis and do the following:
1. Filter any notices for full closures, and filter to a specific geographic polygon.
2. Determine if the filtered list has anything new that has not been notified on.
3. Post whatever is new to a discord webhook
4. Post another update when it notices the closure is removed by BC511.
5. Periodically clean up the database for old alerts that it no longer needs to track.


## What technologies does this use?
This script is designed to be deployed as a serverless lambda function in AWS. It uses the following key technologies:
* Python 3.9 - for the code itself
* Python helper libraries - primarily for some time calcualtion things, connecting to discord, talking to AWS resources, etc
* AWS DynamoDB - for storage of events so we can keep track of what we have seen already and what needs to be updated.
* AWS Lambda - to run the actual code on a periodic basis in the cloud

## How do I use this?
The basic steps would be:
1. Setup an AWS account if you don't have one already
2. Setup a DynamoDB table to for this script to use
3. Setup the necessary IAM roles and policies to allow access from this script to the DB as well as potentially deployment credentials to setup GitHub Actions to be able to autodeploy changes.
4. Create a Lambda Function
5. Build and package this code and deploy to the Lambda function
6. Setup some way to trigger Lambda periodically (probably like AWS event bus or something similar)
