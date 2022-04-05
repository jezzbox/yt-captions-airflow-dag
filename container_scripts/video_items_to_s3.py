import webvtt
from datetime import datetime
import sys
import boto3
import json
from io import StringIO


FROM_S3_PREFIX = 'youtube/task-output--api-endpoint--videos/videos_endpoint_'
TO_S3_PREFIX = 'youtube/task-output--ecs--load-video-items/video_items_'
BUCKET = 'vocabulazy-airflowbucket'

def get_video_timestamp(time_string) -> float:
    '''
    Takes the timestamp of the captions and converts
    to seconds relative to video.
    '''
    converted_time_string = datetime.strptime(time_string[:8], "%H:%M:%S")
    time_diff = converted_time_string - datetime(1900,1,1)
    return time_diff.total_seconds()


client = boto3.client('s3')

# the data interval will be passed through from command line
data_interval_id = sys.argv[1]

from_s3_key = FROM_S3_PREFIX + data_interval_id + '.json'
s3_object = client.get_object(Bucket=BUCKET,Key=from_s3_key)

# gets list of subtitles in S3 for this data interval
subtitles_list = client.list_objects_v2(
    Bucket=BUCKET,MaxKeys=50,
    Prefix='youtube/task-output--ecs--load-captions/' + data_interval_id
)

videos = json.loads(s3_object['Body'].read())

video_items = []
# matches subtitle file with metadata
# converts subtitle file into a list of dictionaries
# combines metadata and subtitle into one item,
# then adds item to video_list
# list is then loaded into S3
if 'Contents' in subtitles_list:
    for subtitle_file in subtitles_list['Contents']:
        sub_key = subtitle_file['Key']
        video_id = sub_key.split(data_interval_id + '_')[1].split('.')[0]
        video = next((item for item in videos if item['id'] == video_id), None)
        if video:
            s3_object = client.get_object(
                Bucket='vocabulazy-airflowbucket',
                Key=sub_key)
            
            captions_list = []

            # read subtitle file into a buffer
            buffer = StringIO(s3_object['Body'].read().decode('utf-8'))
            # use webvtt package to parse subtitle file
            for caption in webvtt.read_buffer(buffer):
                start = get_video_timestamp(caption.start)
                end = get_video_timestamp(caption.end)
                text = caption.text.replace('&nbsp;',' ').replace('  ',' ') \
                              .strip()
                caption_item = {'start':start,'end':end, 'text':text}
                captions_list.append(caption_item)

            video['source'] = 'youtube'
            video['captions'] = captions_list
            video_items.append(video)
        
    to_s3_key = TO_S3_PREFIX + data_interval_id + '.json'
    client.put_object(Body=json.dumps(video_items), Bucket=BUCKET, 
                      Key=to_s3_key)
else:
    pass