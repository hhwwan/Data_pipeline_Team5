import requests
import csv
import boto3
import io
import os
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
API_KEY = os.getenv('API_KEY')
BUCKET_NAME = "airflow-project3"
REDSHIFT_DSN = os.getenv('REDSHIFT_DSN')
AWS_ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
AWS_ROLE = os.getenv('AWS_ROLE')

def extract_box_office_data(target_date):
    """
    API에서 일별 박스오피스 데이터를 가져와 메모리 상에서 CSV 형식으로 반환
    """
    print("API에서 데이터 수집 중...")
    url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    params = {
        "key": API_KEY,
        "targetDt": target_date
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        box_office_result = data.get("boxOfficeResult", {})
        daily_box_office_list = box_office_result.get("dailyBoxOfficeList", [])

        if not daily_box_office_list:
            print("박스오피스 데이터가 비어 있습니다. API 응답:", data)
            return None
        
        # boxOfficeResult의 추가 데이터 가져오기
        boxoffice_type = box_office_result.get("boxofficeType", "")
        show_range = box_office_result.get("showRange", "")

        # 각 항목에 추가 데이터 병합
        for entry in daily_box_office_list:
            entry["boxOfficeType"] = boxoffice_type
            entry["showRange"] = show_range
        
        # 메모리 상에서 CSV 파일 생성
        output = io.StringIO()
        fieldnames = daily_box_office_list[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for entry in daily_box_office_list:
            writer.writerow(entry)

        output.seek(0)
        return output.getvalue()
    else:
        raise Exception(f"API 호출 실패: {response.status_code}")

def update_csv_headers(csv_data, target_date):
    """
    기존 CSV 데이터의 헤더를 새로운 헤더로 변경 및 날짜 수정
    """
    print("CSV 파일 헤더 변경 중...")
    original_headers = [
        'rnum', 'rank', 'rankInten', 'rankOldAndNew', 'movieCd', 'movieNm', 'openDt', 
        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 'audiCnt', 
        'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt','boxOfficeType','showRange'
    ]

    # new_headers에 target_date 적용
    new_headers = [
        f'{target_date}_rank_num', f'{target_date}_ranking', f'{target_date}_increase_decrease', 
        f'{target_date}_new_entry', 'code', 'title', 
        'released_date', f'{target_date}_sales', f'{target_date}_sales_ratio', 
        f'{target_date}_sales_increase_decrease', f'{target_date}_sales_increase_decrease_ratio', 
        f'{target_date}_total_sales', f'{target_date}_audience_num', 
        f'{target_date}_audience_increase_decrease', f'{target_date}_audience_increase_decrease_ratio', 
        f'{target_date}_total_audience_num', f'{target_date}_screen_num', 
        f'{target_date}_screen_show', 'boxOfficeType', 'showRange'
    ]

    # CSV 데이터 수정
    lines = csv_data.splitlines()
    lines[0] = ','.join(new_headers)

    # 'showRange' 값 수정 (20250123~20250123 -> 20250123)
    updated_lines = []
    for line in lines[1:]:
        columns = line.split(',')
        show_range_index = new_headers.index('showRange')  # 'showRange'가 몇 번째 열인지 확인
        show_range_value = columns[show_range_index]
        
        # '~'을 기준으로 분리하고 첫 번째 날짜만 남기기
        if '~' in show_range_value:
            columns[show_range_index] = show_range_value.split('~')[0]
        
        updated_lines.append(','.join(columns))

    # 수정된 데이터 합치기
    return '\n'.join([lines[0]] + updated_lines)

def upload_to_s3(csv_data, s3_file_name):
    print("S3에 데이터 업로드 중...")
    
    s3_client = boto3.client('s3', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    
    file_obj = io.BytesIO(csv_data.encode('utf-8'))
    s3_client.upload_fileobj(file_obj, BUCKET_NAME, s3_file_name)
    print(f"파일이 S3에 업로드되었습니다: {BUCKET_NAME}/{s3_file_name}")

def create_redshift_table(table_name, target_date):
    """
    새로운 Redshift 테이블을 생성 (이미 존재하면 생성하지 않음)
    """
    print(f"Redshift 테이블 생성 중: {table_name}")
    conn = psycopg2.connect(REDSHIFT_DSN)
    cur = conn.cursor()
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw_data."{table_name}" (
        "{target_date}_rank_num" INTEGER NOT NULL,
        "{target_date}_ranking" INTEGER NOT NULL,
        "{target_date}_increase_decrease" INTEGER,
        "{target_date}_new_entry" VARCHAR(50),
        code NUMERIC,
        title VARCHAR(255) NOT NULL,
        released_date DATE,
        "{target_date}_sales" NUMERIC,
        "{target_date}_sales_ratio" FLOAT,
        "{target_date}_sales_increase_decrease" bigint,
        "{target_date}_sales_increase_decrease_ratio" FLOAT,
        "{target_date}_total_sales" NUMERIC,
        "{target_date}_audience_num" NUMERIC,
        "{target_date}_audience_increase_decrease" NUMERIC,
        "{target_date}_audience_increase_decrease_ratio" FLOAT,
        "{target_date}_total_audience_num" NUMERIC,
        "{target_date}_screen_num" INTEGER,
        "{target_date}_screen_show" INTEGER,
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
        delimiter ',' dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 removequotes;
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
    'daily_csv_pipeline',
    default_args=default_args,
    description='일간 박스오피스 데이터 수집 및 S3 업로드 파이프라인',
    schedule_interval='0 0 * * *',  # 매일 오전 9시에 실행
) as dag:

    # 태스크 정의
    yesterday = datetime.now() - timedelta(days=1)
    target_date = yesterday.strftime("%Y%m%d")
    s3_file_name = f"daily_box_office_data_{target_date}.csv"
    s3_path = f"s3://{BUCKET_NAME}/{s3_file_name}"
    table_name = f"{target_date}_box_office"

    task_extract_data = PythonOperator(
        task_id='extract_box_office_data',
        python_callable=extract_box_office_data,
        op_args=[target_date],
    )

    task_update_headers = PythonOperator(
        task_id='update_csv_headers',
        python_callable=update_csv_headers,
        op_args=["{{ ti.xcom_pull(task_ids='extract_box_office_data') }}", target_date],
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='update_csv_headers') }}",s3_file_name],
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
