## task 1

https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz
rows in raw dataset
NUMBER OF ROWS IN green_tripdata_2020-01 RAW FILE: 447770. 


## task 2

create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

0 5 1 * *


## task 3

Using etl_gcs_to_bq.py as a starting point, 
modify the script for extracting data from GCS and loading it into BigQuery. 
This new script should not fill or remove rows with missing values.
(The script is really just doing the E and L parts of ETL).


The main flow should print the total number of rows processed by the script.
Set the flow decorator to log the print statement.


Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. 
Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

>>14851920
>> 14 851 920

14,851,920
12,282,990
27,235,753
11,338,483

## task 4

[//]: # (Run your deployment in a local subprocess &#40;the default if you don’t specify an infrastructure&#41;. Use the Green taxi data for the month of November 2020.)

[//]: # (How many rows were processed by the script?)
>>88605.




## task 5

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

How many rows were processed by the script?

125,268
377,922
728,390
514,392

>> 514392.

## task 6

******** (8)

5
6
8
10

Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97




💻 Week #2 of my #LearningInPublic journey is all about Workflow Orchestration! 
Even though I am more used to Airflow, this week I learned to work with Prefect:
* Installing Prefect
* Downloading data and loading it in Parquet files to DataLake - Google Cloud Storage 
* Moving data from GCP to Google DataWarehouse (BigQuerry)
* Parametrizing and scheduling flows
* Creating a deployment locally
* Setting up Prefect Agent
* Running tasks in Docker
* and much more 

#dataengineering #Prefect #WorkflowOrchestration #ETL #dataanalytics 

Thank you, Jeff Hale and Anna Geller, for very informative lectures about the Prefect!

https://app.prefect.cloud/
