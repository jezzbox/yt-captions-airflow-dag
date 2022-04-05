# Airflow DAG example: inserting youtube captions into MongoDB
An airflow dag that gets videos from the youtube api together with their captions and inserts into a MongoDB collection.

### Introduction
This repo is to display a DAG that I have created in airflow.
the DAG makes a call to the google youtube API every 4 hours to get videos from Brazil that have subtitles.
The subtitles along with the video metadata are then upserted into a MongoDb collection.
Throughout this process, AWS S3 is used as a staging area.

----------------------------------
### Tasks
The tasks run as follows:

* dag_start
* search_endpoint_task
* videos_endpoint_task
* captions_to_s3_ecs_task
* video_items_ecs_task
* upsert_video_items_task


## dag_start
This is a dummy task to identify the start of the dag and does nothing else.

## search_endpoint_task
This is the first call to the youtube API. the search endpoint is used to identify videos based on a set of parameters.

| Parameter         | Value                  |
| ------------------| -----------------------|
| key               | *api_key*              |
| part              | snippet                |
| type              | video                  |
| maxResults        | 50                     |
| order             | date                   |
| location          | -23.533773, -46.625290 |
| locationRadius    | 1000km                 |
| publishedAfter    | *data_interval_start*  |
| publishedBefore   | *data_interval_end*    |
| safeSearch        | none                   |
| videoCaption      | closedCaption          |
| relevanceLanguage | pt                     |
| regionCode        | BR                     |

This set of parameters gave me the best results.
The location is the coordinates of Sao Paulo, as with the 1000km radius this should cover most of the southeast region of Brazil.

The results are then saved into an S3 bucket.

*note: The reason we can't just use the videos endpoint straight away is because the videos endpoint doesnt have these parameter options available*

## videos_endpoint_task
This is the second call to the youtube API. It first gets the object from the previous task from S3, takes the video ids and uses them
for the *video_ids* parameter.

The result is then filtered to remove any data that we don't want, and inserted into S3.

### captions_to_s3_ecs_task
*script: container_scripts/captions_to_s3.py*


This task runs a docker container on AWS ECS. 

The container runs a python script that utilizes the youtube_dl python package to
download the captions for the videos returned in the task 'search_endpoint_task'.
Whilst the youtube API does have a download caption option, it does not work the 
majority of the time which is why youtube_dl is necessary for this step.

Again, the subtitle files are saved into the S3 bucket.

*ECS was used for this step as I felt it was too much to do within airflow*

### captions_to_s3_ecs_task
*script: container_scripts/captions_to_s3.py*


This task runs a docker container on AWS ECS.


This container is the transformation step, it runs a python script 
that converts the subtitle files into a list object and combines it with the video metadata from the videos endpoint.
this is then uploaded into S3 for a final time.


*ECS was used for this step as I felt it was too much to do within airflow*


### upsert_video_items_task

This final task gets the file object from the previous step and upserts into a MongoDB collection.
