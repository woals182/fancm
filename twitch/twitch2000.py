from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
  'start_date': datetime(2023, 1, 1),
}

def filecall():
    import pandas as pd

    file_path = 'twitch_2000.csv'
    df_channel_name = pd.read_csv(file_path)
    
    return df_channel_name


def twitch_tracker(**context):
    import pandas as pd
    import requests
    import time
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d")
    df_channel_name = context['task_instance'].xcom_pull(task_ids='channel_name')

    new_data_frame = {
            'id' : [],
            'Avg_viewers' : [],
            'Time_streamed_min' : [],
            'Time_peak_viewers' : [],
            'Hours_watched' : [],
            'Rank' : [],
            'Follower_gained' : [],
            'Total_followers' : [],
            'Total_views' : [],
            'Get_data' : []}

    for i in df_channel_name['name']:
        try:
        ##############################
            name = i.split("/")[-1]

            time.sleep(1)
            tracker_url = f'https://twitchtracker.com/api/channels/summary/{name}'

        #############################################################

            summary = requests.get(tracker_url).json()


        # ##################################################################
            print(f'{name} 님 :' ,summary)


            time.sleep(2)


            new_data_frame['id'].append(name)
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
            df_tracker.to_csv('twitchtop2000.csv',index=False,encoding='utf-8')
    return df_tracker

def bigquery():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pandas as pd
    # GCP 프로젝트 ID와 빅쿼리 데이터셋 ID를 지정
    project_id = 'GCP 프로젝트 ID'
    dataset_id = '빅쿼리 데이터셋 ID'

    service_account_path = '서비스 계정 json 위치'

    # 서비스 계정 키 파일을 로드하여 인증 정보를 얻기
    credentials = service_account.Credentials.from_service_account_file(service_account_path)

    # 빅쿼리 클라이언트 객체를 생성
    client = bigquery.Client(project=project_id, credentials=credentials)

    # concat return값 불러오기
    data = pd.read_csv('twitchtop2000.csv')

    # 데이터프레임을 빅쿼리에 업로드
    table_name = 'twitch_top_2000'
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    job_config = bigquery.LoadJobConfig(
          source_format=bigquery.SourceFormat.CSV,
          autodetect=True,
          skip_leading_rows=1

    )
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()  # 작업 완료 대기


with DAG(dag_id = 'twitchtracker2000',
         schedule_interval= '30 18 * * *',
         default_args = default_args,
         tags = ['twitch_tracker_api'],
         catchup =False) as dag:

    channel_name = PythonOperator(
        task_id = 'channel_name',
        python_callable = filecall,
        queue = 'queue2'
    )

    api_twitchtracker = PythonOperator(
        task_id = 'twitch_tracker_api',
        python_callable = twitch_tracker,
        queue = 'queue2'
    )
    bigquery = PythonOperator(
        task_id = 'bigquery',
        python_callable = bigquery,
        queue = 'queue2'
    )

channel_name >> api_twitchtracker >> bigquery
