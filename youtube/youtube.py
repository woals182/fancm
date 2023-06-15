from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def bigquery(**context):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pandas as pd
    # GCP 프로젝트 ID와 빅쿼리 데이터셋 ID를 지정
    project_id = 'GCP 프로젝트 ID'
    dataset_id = '빅쿼리 데이터셋 ID'

    service_account_path = '서비스 계정 json 파일'

    # 서비스 계정 키 파일을 로드하여 인증 정보를 얻기
    credentials = service_account.Credentials.from_service_account_file(service_account_path)

    # 빅쿼리 클라이언트 객체를 생성
    client = bigquery.Client(project=project_id, credentials=credentials)

    # 데이터 불러오기
    data = pd.read_csv('youtube_data.csv')
    
    # 데이터프레임을 빅쿼리에 업로드
    table_name = 'youtube_data'
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    job_config = bigquery.LoadJobConfig(
                autodetect=True,
                allow_quoted_newlines=True,
                source_format=bigquery.SourceFormat.CSV
                )
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()  # 작업 완료 대기


default_args = {
  'start_date': datetime(2023, 1, 1),
}

path = 'script 위치'

with DAG(dag_id = 'youtube',
         schedule_interval= '20 21 * * *',
         default_args = default_args,
         tags = ['crawl'],
         catchup =False) as dag:

    
    video_id = BashOperator(
            task_id = 'video_id',
            bash_command = f'python {path}/youtube_video_id.py',
            queue = 'queue2')
    
    concat = BashOperator(
            task_id = 'concat',
            bash_command = f'python {path}/youtube_video_channel_concat.py',
            queue = 'queue2')

    extract_data = BashOperator(
            task_id = 'youtube_data',
            bash_command = f'python {path}/youtube_data.py',
            queue = 'queue2')

    bigquery = PythonOperator(
            task_id = 'bigquery',
            python_callable = bigquery,
            queue = 'queue2')

video_id >> concat >> extract_data >> bigquery
