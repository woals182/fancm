from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator


default_args = {
  'start_date': datetime(2023, 1, 1),
}

def filecall():
    import pandas as pd

    file_path = '파일 위치/twitch_channel_name.csv'
    df_channel_name = pd.read_csv(file_path)

    return df_channel_name
##############################################
def twitch_api(**context):
    import pandas as pd
    import requests
    import time

    df_channel_name = context['task_instance'].xcom_pull(task_ids='channel_name')

    client_id = "트위치 api 클라이언트 id"
    oauth_token = "토큰"

    user_headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth_token}"}

    data = {
        "channel_name" : [],
        "user_id" : [],
        "display_name" : [],
        "view_count" : [],
        "follower" : [],
        "created_date" : [],
        "category" : [],
        "tag" : []
        }

    for channel_name in df_channel_name['channel_name']:
        try:

            user_url = f"https://api.twitch.tv/helix/users?login={channel_name}"

            user_response = requests.get(user_url, headers=user_headers).json()
            user_id = user_response['data'][0]['id']
            display_name = user_response['data'][0]['display_name']
            view_count = user_response['data'][0]['view_count']
            created_data = user_response['data'][0]['created_at']
            time.sleep(1)

            #########################################
            channel_url = f"https://api.twitch.tv/helix/channels?broadcaster_id={user_id}"

            channel_response = requests.get(channel_url, headers=user_headers).json()
            broadcating_category = channel_response['data'][0]['game_name']
            tags = channel_response['data'][0]['tags']
            time.sleep(1)

            follows_url = f'https://api.twitch.tv/helix/users/follows?to_id={user_id}'

            follows_response = requests.get(follows_url, headers=user_headers).json()
            follwer_count = follows_response['total']
            time.sleep(1)

            ###########################

            data['channel_name'].append(channel_name)
            data['user_id'].append(user_id)
            data['display_name'].append(display_name)
            data['view_count'].append(view_count)
            data['created_date'].append(created_data)
            data['category'].append(broadcating_category)
            data['tag'].append(tags)
            data['follower'].append(follwer_count)
            ############################
            time.sleep(1)
            print(f'{channel_name}: 데이터 수집 완료')
        except:
            pass

        finally:
            df_twitch = pd.DataFrame(data)

    context['task_instance'].xcom_push(key='df_twitch', value=df_twitch)

    return df_twitch
#####################################################################
def twitch_tracker(**context):
    import pandas as pd
    import requests
    import time
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d")
    df_channel_name = context['task_instance'].xcom_pull(task_ids='channel_name')

    new_data_frame = {
    'channel_name' : [],
    'Avg_viewers' : [],
    'Time_streamed_min' : [],
    'Time_peak_viewers' : [],
    'Hours_watched' : [],
    'Rank' : [],
    'Follower_gained' : [],
    'Total_followers' : [],
    'Total_views' : [],
    'Get_data' : []}

    for name in df_channel_name['channel_name']:
        try:
        ##############################
            time.sleep(1)
            tracker_url = f'https://twitchtracker.com/api/channels/summary/{name}'

        #############################################################

            summary = requests.get(tracker_url).json()

        
        # ##################################################################
            print(f'{name} 님 :' ,summary)


            time.sleep(2)


            new_data_frame['channel_name'].append(name)
            if summary:
                new_data_frame['Avg_viewers'].append(summary['avg_viewers'])
                new_data_frame['Time_streamed_min'].append(summary['minutes_streamed'])
                new_data_frame['Time_peak_viewers'].append(summary['max_viewers'])
                new_data_frame['Hours_watched'].append(summary['hours_watched'])
                new_data_frame['Rank'].append(summary['rank'])
                new_data_frame['Follower_gained'].append(summary['followers'])
                new_data_frame['Total_followers'].append(summary['followers_total'])
                new_data_frame['Total_views'].append(summary['views_total'])
                new_data_frame['Get_data'].append(now)
            else:
                new_data_frame['Avg_viewers'].append(None)
                new_data_frame['Time_streamed_min'].append(None)
                new_data_frame['Time_peak_viewers'].append(None)
                new_data_frame['Hours_watched'].append(None)
                new_data_frame['Rank'].append(None)
                new_data_frame['Follower_gained'].append(None)
                new_data_frame['Total_followers'].append(None)
                new_data_frame['Total_views'].append(None)
                new_data_frame['Get_data'].append(now)
            time.sleep(1)

        except:
            pass

        finally:
            df_tracker = pd.DataFrame(new_data_frame)
    
    context['task_instance'].xcom_push(key='df_tracker',value=df_tracker)
    
    return df_tracker
#########################################################
def concat(**context):
    import pandas as pd

    df_channel_name = context['task_instance'].xcom_pull(task_ids='channel_name')
    df_twitch = context['task_instance'].xcom_pull(key='df_twitch')
    df_tracker = context['task_instance'].xcom_pull(key='df_tracker')


    merged_df = pd.merge(df_channel_name, df_twitch, on='channel_name', how='inner')

    merged_df.drop_duplicates(subset='channel_name', inplace=True)

    merged_df_ = pd.merge(merged_df, df_tracker, on='channel_name', how='inner')    
    
    merged_df_.drop_duplicates(subset='channel_name', inplace=True)

    merged_df_.drop("Unnamed: 0", axis=1, inplace=True)

    merged_df_.to_csv('파일 저장 위치/twitch.csv',index=False,encoding='utf-8')

    return merged_df_
##########################################################
def bigquery(**context):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pandas as pd
    # GCP 프로젝트 ID와 빅쿼리 데이터셋 ID를 지정
    project_id = 'GCP 프로젝트 ID'
    dataset_id = '빅쿼리 데이터셋 ID'

    service_account_path = '서비스 계정 json 파일 위치'

    # 서비스 계정 키 파일을 로드하여 인증 정보를 얻기
    credentials = service_account.Credentials.from_service_account_file(service_account_path)

    # 빅쿼리 클라이언트 객체를 생성
    client = bigquery.Client(project=project_id, credentials=credentials)

    # 데이터 불러오기
    data = pd.read_csv('twitch.csv')

    # 데이터프레임을 빅쿼리에 업로드
    table_name = 'twitch_test'
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    job_config = bigquery.LoadJobConfig(
          source_format=bigquery.SourceFormat.CSV,
          autodetect=True,
          skip_leading_rows=1

    )
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()  # 작업 완료 대기




with DAG(dag_id = 'twitch',
         schedule_interval= '30 15 * * *',
         default_args = default_args,
         tags = ['api_using'],
         catchup =False) as dag:

    channel_name = PythonOperator(
        task_id = 'channel_name',
        python_callable = filecall,
        queue = 'queue2'
    )

    api_twitch = PythonOperator(
        task_id = 'twitch_api',
        python_callable = twitch_api,
        queue = 'queue2'
    )

    api_twitchtracker = PythonOperator(
        task_id = 'twitchtracker_api',
        python_callable = twitch_tracker,
        queue = 'queue2'
    )

    concat = PythonOperator(
        task_id = 'concat',
        python_callable = concat,
        queue = 'queue2'
    )

    bigquery = PythonOperator(
        task_id = 'bigquery',
        python_callable = bigquery,
        queue = 'queue2'
    )
    
    

channel_name >>[api_twitch,api_twitchtracker] >> concat >> bigquery

