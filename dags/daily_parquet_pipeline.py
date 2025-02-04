import requests
import pandas as pd
import io
import os
import json
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

load_dotenv()

API_KEY = os.getenv('API_KEY')
AWS_CONN_ID = "aws_default"
BUCKET_NAME = "airflow-project3"
REDSHIFT_DSN = os.getenv('REDSHIFT_DSN')
AWS_ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
AWS_ROLE = os.getenv('AWS_ROLE')

def extract_box_office_data(target_date):
    """
    API에서 일별 박스오피스 데이터를 가져와 메모리 상에서 DataFrame 형식으로 반환
    """
    print("API에서 데이터 수집 중...")
    url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    params = {
        "key": API_KEY,
        "targetDt": target_date
    }
    response = requests.get(url, params=params)
    data = response.json()
    box_office_result = data.get("boxOfficeResult", {})
    daily_box_office_list = box_office_result.get("dailyBoxOfficeList", [])
    
    df = pd.DataFrame(daily_box_office_list)
    df["boxOfficeType"] = box_office_result.get("boxofficeType", "")
    df["showRange"] = box_office_result.get("showRange", "")

    return df.to_json(orient="records")

def update_parquet_headers(df_json, target_date):
    """
    기존 DataFrame의 컬럼명을 새로운 형식으로 변경
    """
    print("Parquet 파일 헤더 변경 중...")

    df = pd.read_json(df_json)  # JSON 문자열을 DataFrame으로 변환

    # 컬럼명 변환 딕셔너리
    rename_dict = {
        'rnum': f'{target_date}_rank_num',
        'rank': f'{target_date}_ranking',
        'rankInten': f'{target_date}_increase_decrease',
        'rankOldAndNew': f'{target_date}_new_entry',
        'movieCd': 'code',
        'movieNm': 'title',
        'openDt': 'released_date',
        'salesAmt': f'{target_date}_sales',
        'salesShare': f'{target_date}_sales_ratio',
        'salesInten': f'{target_date}_sales_increase_decrease',
        'salesChange': f'{target_date}_sales_increase_decrease_ratio',
        'salesAcc': f'{target_date}_total_sales',
        'audiCnt': f'{target_date}_audience_num',
        'audiInten': f'{target_date}_audience_increase_decrease',
        'audiChange': f'{target_date}_audience_increase_decrease_ratio',
        'audiAcc': f'{target_date}_total_audience_num',
        'scrnCnt': f'{target_date}_screen_num',
        'showCnt': f'{target_date}_screen_show'
    }
    df.rename(columns=rename_dict, inplace=True)

    if 'released_date' in df.columns:
        df['released_date'] = pd.to_datetime(df['released_date']).dt.date

    # 'showRange' 값 수정 (20250123~20250123 -> 20250123)
    if 'showRange' in df.columns:
        df['showRange'] = pd.to_datetime(df['showRange'].str.split('~').str[0]).dt.date

    return df.to_json(orient="records")  # JSON 문자열로 변환하여 반환

def upload_to_s3(df, key, **kwargs):
    """
    XCom에서 전달된 JSON 데이터를 DataFrame으로 변환 후 S3에 업로드
    """
    df = pd.read_json(df)  # 문자열 형태의 JSON을 DataFrame으로 변환

    df = df.astype({
        'released_date': 'datetime64[ns]',
        'showRange': 'datetime64[ns]'
    })

    # DataFrame을 Parquet 형식으로 변환
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)

    # S3 업로드
    S3Hook(AWS_CONN_ID).load_bytes(parquet_buffer.getvalue(), key, BUCKET_NAME, replace=True)

    print(f"파일이 S3({BUCKET_NAME}/{key})에 성공적으로 업로드되었습니다.")

def create_redshift_table(table_name, target_date):
    """
    새로운 Redshift 테이블을 생성 (이미 존재하면 생성하지 않음)
    """
    print(f"Redshift 테이블 생성 중: {table_name}")
    conn = psycopg2.connect(REDSHIFT_DSN)
    cur = conn.cursor()
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw_data."{table_name}" (
        "{target_date}_rank_num" BIGINT NOT NULL,
        "{target_date}_ranking" BIGINT NOT NULL,
        "{target_date}_increase_decrease" BIGINT,
        "{target_date}_new_entry" VARCHAR(50),
        code BIGINT,
        title VARCHAR(255) NOT NULL,
        released_date DATE,
        "{target_date}_sales" BIGINT,
        "{target_date}_sales_ratio" FLOAT,
        "{target_date}_sales_increase_decrease" BIGINT,
        "{target_date}_sales_increase_decrease_ratio" FLOAT,
        "{target_date}_total_sales" BIGINT,
        "{target_date}_audience_num" BIGINT,
        "{target_date}_audience_increase_decrease" BIGINT,
        "{target_date}_audience_increase_decrease_ratio" FLOAT,
        "{target_date}_total_audience_num" BIGINT,
        "{target_date}_screen_num" BIGINT,
        "{target_date}_screen_show" BIGINT,
        boxOfficeType VARCHAR(255),
        showRange DATE NOT NULL
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print("테이블 생성 완료")

def load_to_redshift(s3_path, table_name):
    """
    Redshift 테이블로 적재
    """
    print(f"{table_name} 테이블에 데이터 적재 중...")
    conn = psycopg2.connect(REDSHIFT_DSN)
    cur = conn.cursor()

    copy_query = f"""
        COPY raw_data."{table_name}"
        FROM '{s3_path}'
        credentials 'aws_iam_role=arn:aws:iam::{AWS_ACCOUNT_ID}:role/{AWS_ROLE}'
        FORMAT AS PARQUET;
    """
    cur.execute(copy_query)
    conn.commit()
    cur.close()
    conn.close()
    print("Redshift 데이터 적재 완료")

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'daily_test_data_pipeline',
    default_args=default_args,
    description='일간 박스오피스 데이터 수집 및 S3 업로드 파이프라인',
    schedule_interval='0 0 * * *',  # 매일 오전 9시에 실행
) as dag:

    yesterday = datetime.now() - timedelta(days=1)
    target_date = yesterday.strftime("%Y%m%d")
    s3_file_name = f"daily_box_office_data_{target_date}.parquet"
    s3_path = f"s3://{BUCKET_NAME}/{s3_file_name}"
    table_name = f"{target_date}_box_office"

    task_extract_data = PythonOperator(
        task_id='extract_box_office_data',
        python_callable=extract_box_office_data,
        op_args=[target_date],
    )

    task_update_headers = PythonOperator(
        task_id='update_parquet_headers',
        python_callable=update_parquet_headers,
        op_args=["{{ ti.xcom_pull(task_ids='extract_box_office_data') }}", target_date],
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='update_parquet_headers') }}",s3_file_name],
    )

    task_create_redshift_table = PythonOperator(
        task_id='create_redshift_table',
        python_callable=create_redshift_table,
        op_args=[table_name, target_date],
    )

    task_load_to_redshift = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_args=[s3_path, table_name],
    )

    # 태스크 실행 순서 지정
    task_extract_data >> task_update_headers >> task_upload_to_s3 >> task_create_redshift_table >> task_load_to_redshift
