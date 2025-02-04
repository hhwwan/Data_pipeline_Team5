from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json

def generate_dbt_models(**kwargs):
    """
    자동으로 날짜별 박스오피스 테이블 리스트를 생성하고 XCom을 통해 전달
    """
    # 날짜 범위를 설정 (예: 최근 7일)
    end_date = datetime.now() - timedelta(days=1)  # 어제 날짜
    start_date = end_date - timedelta(days=8)  # 8일 전 (즉, 일주일치)
    
    # 날짜 리스트 생성 (문자열 형식 'YYYYMMDD')
    date_list = [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date - start_date).days + 1)]
    
    # 테이블 이름 생성
    table_names = [f"{date}_box_office" for date in date_list]
    
    # XCom에 JSON 형식으로 저장 (직렬화)
    kwargs['ti'].xcom_push(key='table_names', value=table_names)

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# DAG 정의
with DAG(
    'dbt_box_office_data',
    default_args=default_args,
    description='DBT 이용하여 테이블 만들기',
    schedule_interval='5 0 * * *',  # UTC기준 매일 오전 9시에 실행
    catchup=False,
) as dag:
    
    # DBT 모델 생성 작업
    generate_dbt_sql = PythonOperator(
        task_id='generate_dbt_sql',
        python_callable=generate_dbt_models
    )

    # DBT 실행 작업
    dbt_transform_data = BashOperator(
        task_id='run_dbt_transform_data',
        bash_command="""
        table_names="{{ ti.xcom_pull(task_ids='generate_dbt_sql', key='table_names') }}"
        dbt run --project-dir /opt/airflow/dbt_project/dbt_project/dbt_project \
            --profiles-dir /home/airflow/.dbt \
            --select dbt_project.transform.box_office_data \
            --vars '{"table_names":{{ ti.xcom_pull(task_ids="generate_dbt_sql", key="table_names") | tojson }} }'
        """,
        env={
            'DBT_PROFILES_DIR': '/home/airflow/.dbt',
            'PATH': '/home/airflow/.local/bin:$PATH',
            'PYTHONPATH': '/home/airflow/.local/lib/python3.12/site-packages:$PYTHONPATH',
        }
    )

    dbt_transform_showrange = BashOperator(
        task_id='run_dbt_transform_showrange',
        bash_command="""
        table_names="{{ ti.xcom_pull(task_ids='generate_dbt_sql', key='table_names') }}"
        dbt run --project-dir /opt/airflow/dbt_project/dbt_project/dbt_project \
            --profiles-dir /home/airflow/.dbt \
            --select dbt_project.transform.box_office_showrange \
            --vars '{"table_names":{{ ti.xcom_pull(task_ids="generate_dbt_sql", key="table_names") | tojson }} }'
        """,
        env={
            'DBT_PROFILES_DIR': '/home/airflow/.dbt',
            'PATH': '/home/airflow/.local/bin:$PATH',
            'PYTHONPATH': '/home/airflow/.local/lib/python3.12/site-packages:$PYTHONPATH',
        }
    )

    # 태스크 간 의존성 설정
    generate_dbt_sql >> dbt_transform_data >> dbt_transform_showrange
