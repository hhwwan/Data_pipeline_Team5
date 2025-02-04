# Apache Airflow 2.9.1 공식 이미지 사용
FROM apache/airflow:2.9.1

# python-dotenv 설치
RUN pip install python-dotenv

# dbt 및 Redshift 어댑터 설치
RUN pip install --no-cache-dir dbt-core dbt-redshift

# jq 설치 (루트 권한으로 실행)
USER root
RUN apt-get update && apt-get install -y jq

# .env 파일을 컨테이너에 복사
COPY .env /opt/airflow/.env

# 기본 사용자로 돌아가도록 설정
USER airflow
