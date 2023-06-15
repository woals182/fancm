from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
def africa_to_bigquery():
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
    data = pd.read_csv('africa_data.csv')

    # 데이터프레임을 빅쿼리에 업로드
    table_name = 'africa'
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    job_config = bigquery.LoadJobConfig(
          source_format=bigquery.SourceFormat.CSV,
          autodetect=True,
          skip_leading_rows=1

    )
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()  # 작업 완료 대기



default_args = {
  'start_date': datetime(2023, 1, 1),
}

path = 'script 파일 위치'

with DAG(dag_id = 'africa',
         schedule_interval= '00 16 * * *',
         default_args = default_args,
         tags = ['crawl'],
         catchup =False) as dag:

    crawl_bj_id = BashOperator(
        task_id = 'crawl_bj_id',
        bash_command = f'python {path}/africa_bj_id.py',
        queue = 'queue1'
    )

    crawl_bj_data = BashOperator(
        task_id = 'crawl_bj_data',
        bash_command = f'python {path}/africa_data.py',
        queue = 'queue1'
    )

    africa_to_bigquery = PythonOperator(
        task_id = 'africa_to_bigquery',
        python_callable = africa_to_bigquery,
        queue = 'queue1'
    )
    

crawl_bj_id >> crawl_bj_data >> africa_to_bigquery
