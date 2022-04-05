from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.http_hook import HttpHook
from airflow.providers.amazon.aws.operators.ecs import EcsOperator
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

default_args = {
    'owner':'airflow',
    'depend_on_past':False,
    'start_date': datetime(2022,3,2),
    'retries':3,
    'retry_delay':timedelta(minutes=5)
}

SEARCH_PREFIX = 'youtube/task-output--api-endpoint--search/search_endpoint_'
VIDEOS_PREFIX = 'youtube/task-output--api-endpoint--videos/videos_endpoint_'
VIDEO_ITEMS_PREFIX = 'youtube/task-output--ecs--load-video-items/video_items_'
BUCKET = 'vocabulazy-airflowbucket'

def load_search_endpoint(data_interval_start,data_interval_end,
    data_interval_id,yt_api_key) -> str:
    '''
    Calls the google youtube api at the 'search' endpoint
    and loads the results into S3.
    '''
    api_hook = HttpHook(http_conn_id='youtube_data_api',method='GET')

    parameters = {'key':yt_api_key,'part':'snippet','type':'video',
        'maxResults':'50','order':'date','location':'-23.533773, -46.625290',
        'locationRadius':'1000km', 'publishedAfter':data_interval_start,
        'publishedBefore':data_interval_end, 'safeSearch':'none',
        'videoCaption':'closedCaption', 'relevanceLanguage':'pt',
        'regionCode':'BR'}

    response = api_hook.run(endpoint='search',
        data = parameters,
        headers={"Content-Type": "application/json"})

    content = json.loads(response.content)
    video_ids = [item['id']['videoId'] for item in content['items']]

    filename = SEARCH_PREFIX + data_interval_id + '.json'

    s3 = S3Hook('s3_conn')
    s3.load_string(json.dumps(video_ids), filename, bucket_name=BUCKET,
                   replace=True)
    return data_interval_id


def load_videos_endpoint(ti, yt_api_key):
    '''
    Calls the google youtube api at the 'videos' endpoint by
    retrieveing a list of video ids from S3.
    the videos endpoint has more information than the search
    endpoint which is why two calls are needed.
    '''
    s3 = S3Hook('s3_conn')
    data_interval_id = ti.xcom_pull(task_ids='load_search_endpoint')
    s3_key = SEARCH_PREFIX + data_interval_id  + '.json'

    s3_object = s3.read_key(s3_key, bucket_name=BUCKET)
    video_ids = json.loads(s3_object)
    comma_separated_string = ','.join(video_ids)
    api_hook = HttpHook(http_conn_id='youtube_data_api',method='GET')
    print(video_ids)
    response = api_hook.run(endpoint='videos',
        data = {
            'key':yt_api_key,
            'part':'snippet,contentDetails,statistics',
            'id':comma_separated_string},
        headers={"Content-Type": "application/json"})

    content = json.loads(response.content)

    videos = []

    #filters each video item to only keep the relevant data.
    for item in content['items']:
        id = item['id']
        title = item['snippet']['title']

        statistics = item['statistics']

        video_details = {}
        for field in ['description','publishedAt',
                      'channelId','channelTitle',
                      'categoryId','defaultLanguage','tags']:
            if field in item['snippet']:
                video_details[field] = item['snippet'][field]
        video_details['duration'] = item['contentDetails']['duration']

        video_dict = {'id':id, 'title':title,
                      'video_details':video_details,
                      'statistics':statistics}
        videos.append(video_dict)
    
    filename = VIDEOS_PREFIX + data_interval_id + '.json'

    s3 = S3Hook('s3_conn')
    response = s3.load_string(json.dumps(videos), filename,bucket_name=BUCKET,
                              replace=True)
    return data_interval_id


def upsert_video_items(ti):
    '''
    Upserts the video items from S3 into mongodb.
    '''
    s3 = S3Hook('s3_conn')
    data_interval_id = ti.xcom_pull(task_ids='load_videos_endpoint')
    s3_key = VIDEO_ITEMS_PREFIX + data_interval_id +'.json'
    key_exists = s3.check_for_key(s3_key, bucket_name=BUCKET)

    if key_exists:
        s3_object = s3.read_key(s3_key, bucket_name=BUCKET)
        video_items= json.loads(s3_object)

        mongo = MongoHook(conn_id='mongodb_conn')
        filters = [{'source': doc['source'],
                    'id':doc['id']} for doc in video_items]
        mongo.replace_many('content', video_items, filter_docs=filters,
                           upsert=True)
    else:
        pass

with DAG('yt_mongodb_insert_videos',
        default_args=default_args,
        schedule_interval='0 */4 * * *',
        max_active_runs=1,
        catchup=True) as dag:
    data_interval_start = "{{ data_interval_start | ts }}"
    data_interval_end = "{{ data_interval_end | ts }}"
    data_interval_id = "{{ data_interval_end | ts_nodash }}"

    yt_api_key = Variable.get("youtube_data_api_key")

    dag_start = DummyOperator(task_id='first_task')

    search_endpoint_task = PythonOperator(task_id='load_search_endpoint',
        python_callable=load_search_endpoint,
        op_kwargs={'data_interval_start':data_interval_start,
                   'data_interval_end':data_interval_end,
                   'data_interval_id':data_interval_id,
                   'yt_api_key':yt_api_key})

    videos_endpoint_task = PythonOperator(task_id='load_videos_endpoint',
        python_callable=load_videos_endpoint,
        op_kwargs={'yt_api_key':yt_api_key})

    captions_to_s3_ecs_task = EcsOperator(
        task_id='run_captions_to_s3_ecs',
        cluster='vocabulazy-cluster-1',
        region_name='eu-west-1',
        task_definition='airflow-task-definition',
        launch_type="FARGATE",
        aws_conn_id="s3_conn",
        overrides={
            'containerOverrides': [
                {
                'name': 'load-youtube-captions',
                'command': ["{{ti.xcom_pull(task_ids='load_search_endpoint')}}"]
            }]},
        network_configuration={
        'awsvpcConfiguration':{
            'assignPublicIp': 'ENABLED',
            'subnets': ['subnet-05704bdc778035586','subnet-0a30207bbe5fff06b']
        }}
        )

    video_items_ecs_task = EcsOperator(
        task_id='run_load_video_items_ecs',
        cluster='vocabulazy-cluster-1',
        region_name='eu-west-1',
        task_definition='load-video-item-task-definition',
        launch_type="FARGATE",
        aws_conn_id="s3_conn",
        overrides={
            'containerOverrides': [
                {
                'name': 'load-video-items',
                'command': ["{{ti.xcom_pull(task_ids='load_videos_endpoint')}}"]
            }]},
        network_configuration={
        'awsvpcConfiguration':{
            'assignPublicIp': 'ENABLED',
            'subnets': ['subnet-05704bdc778035586','subnet-0a30207bbe5fff06b']
        }}
        )

    upsert_video_items_task = PythonOperator(task_id='upsert_video_items',
                                             python_callable=upsert_video_items)
                                             
    dag_start >> search_endpoint_task
    search_endpoint_task >> [videos_endpoint_task,captions_to_s3_ecs_task]
    [videos_endpoint_task,captions_to_s3_ecs_task] >> video_items_ecs_task
    video_items_ecs_task >> upsert_video_items_task