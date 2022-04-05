import boto3
import youtube_dl
from time import sleep
import json
import os
import random
import sys


FROM_S3_PREFIX = 'youtube/task-output--api-endpoint--search/search_endpoint_'
TO_S3_PREFIX = 'youtube/task-output--ecs--load-captions/'
BUCKET = 'vocabulazy-airflowbucket',

def find_subtitle_file(video_id, directory) -> str:
    '''
    Returns the filename associated with the video id.
    '''

    for filename in os.listdir(directory):
        if filename.startswith(video_id + '.'):
            return filename
    return None

client = boto3.client('s3')

# the data interval will be passed through from command line
data_interval_id = sys.argv[1]

#construct s3 key from data interval and s3 prefix
from_s3_key = FROM_S3_PREFIX + data_interval_id + '.json'
s3_object = client.get_object(Bucket = BUCKET,Key=from_s3_key)

#load the json object
videos = json.loads(s3_object['Body'].read())

#for each video_id, download subtitle and upload to s3
for video_id in videos:
    ydl_opts = {'skip_download':True, 'subtitleslangs':['pt'],'forcetitle':True,
                'ignoreerrors':True, 'writesubtitles':True,
                'subtitleslangs':['pt'],'outtmpl':'./temp/' + video_id}

    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(['https://www.youtube.com/watch?v=' + video_id])

    #check to see if a caption file has been downloaded
    # (not all videos have pt captions so need to check)
    filename = find_subtitle_file(video_id, './temp')
    if filename:
        local_dir = './temp/' + filename
        to_s3_key = TO_S3_PREFIX + data_interval_id + '_' + filename
        client.upload_file(local_dir, BUCKET, to_s3_key)
    else:
        pass

    #sleep to avoid too many requests at once
    sleep(random.randint(2,6))