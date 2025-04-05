import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
import requests

DAG_FOLDER = "/opt/airflow/dags"

# ฟังก์ชันดึงข้อมูลจาก API
def _get_air_quality_data():
    API_KEY = "a02c124d-de2a-4dda-845e-e2141f4e1221"  
    url = f"http://api.airvisual.com/v2/city?city=Chatuchak&state=Bangkok&country=Thailand&key={API_KEY}"
    print(f"Requesting URL: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if response.status_code != 200 or data.get("status") != "success":
            raise ValueError(f"API request failed: {data.get('data', {}).get('message', 'Unknown error')}")

        # Save JSON data
        with open(f"{DAG_FOLDER}/air_quality_data.json", "w") as f:
            json.dump(data, f)

        return data  

    except requests.exceptions.RequestException as e:
        print(f"API Request Error: {e}")
        raise ValueError(f"API request failed: {e}")
 


def _validate_data():
    # อ่านข้อมูลจากไฟล์
    with open(f"{DAG_FOLDER}/air_quality_data.json", "r") as f:
        data = json.load(f)

    print("Validating data:", data)  # พิมพ์ข้อมูลก่อนการตรวจสอบ

def _validate_temperature_range():
    try:
        # เปิดไฟล์และโหลดข้อมูล JSON
        with open(f"{DAG_FOLDER}/data.json", "r") as f:
            data = json.load(f)

        # ดึงค่าของ temp จากข้อมูล JSON
        temp = data.get("main", {}).get("temp", None)

        # ตรวจสอบว่า temp มีค่าอยู่หรือไม่
        if temp is None:
            raise ValueError("Temperature data is missing!")

        # ตรวจสอบอุณหภูมิให้อยู่ในช่วงที่ต้องการ
        if not (30 <= temp <= 45):
            raise ValueError(f"Temperature {temp} is out of range.")

        print(f"Temperature {temp} is within the acceptable range.")

    except FileNotFoundError:
        print(f"Error: {DAG_FOLDER}/data.json file not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {DAG_FOLDER}/data.json.")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def _create_weather_table():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS weathers (
            dt BIGINT NOT NULL,
            temp FLOAT NOT NULL,
            feels_like FLOAT
        )
    """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres():
    # เชื่อมต่อกับฐานข้อมูล PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    try:
        # เปิดไฟล์และโหลดข้อมูล
        with open(f"{DAG_FOLDER}/air_quality_data.json", "r") as f:
            data = json.load(f)

        # ดึงค่าที่จำเป็นจากข้อมูล JSON
        temp = data["main"]["temp"]
        feels_like = data["main"]["feels_like"]
        dt = data["dt"]

        # สร้าง SQL query โดยใช้ placeholder เพื่อป้องกัน SQL Injection
        sql = """
            INSERT INTO weathers (dt, temp, feels_like)
            VALUES (%s, %s, %s)
        """
        
        # Execute SQL query และใส่ค่าพารามิเตอร์
        cursor.execute(sql, (dt, temp, feels_like))
        
        # Commit ข้อมูล
        connection.commit()

    except Exception as e:
        # หากเกิดข้อผิดพลาดใด ๆ ให้ rollback
        connection.rollback()
        print(f"Error occurred: {e}")
    
    finally:
        # ปิด cursor และ connection เมื่อเสร็จสิ้น
        cursor.close()
        connection.close()

default_args = {
    "email": ["kan@odds.team"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "api_data",
    default_args=default_args,
    schedule="0 */3 * * *",  # ตั้งเวลาให้ DAG ทำงานทุกๆ 3 ชั่วโมง
    start_date=timezone.datetime(2025, 2, 1),
    tags=["Bangkok"],
):
    start = EmptyOperator(task_id="start")

    # สร้าง PythonOperator สำหรับการดึงข้อมูล
    get_weather_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    # สร้าง PythonOperator สำหรับการ validate ข้อมูล
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    # สร้าง PythonOperator สำหรับ validate อุณหภูมิ
    validate_temperature_range = PythonOperator(
        task_id="validate_temperature_range",
        python_callable=_validate_temperature_range
    )

    # สร้าง PythonOperator สำหรับการสร้างตารางในฐานข้อมูล
    create_weather_table = PythonOperator(
        task_id="create_weather_table",
        python_callable=_create_weather_table,
    )

    # สร้าง PythonOperator สำหรับการบันทึกข้อมูลลงฐานข้อมูล
    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )


    # ส่งอีเมลแจ้งเตือนเมื่อทำงานเสร็จ
    send_email = EmailOperator(
        task_id="send_email",
        to=["kan@odds.team"],
        subject="Finished getting open weather data",
        html_content="Done",
    )

    end = EmptyOperator(task_id="end")

    # สร้างการเชื่อมโยงระหว่าง task ต่างๆ
    start >> get_weather_data >> [validate_data, validate_temperature_range] >> load_data_to_postgres >> send_email
    start >> create_weather_table >> load_data_to_postgres
    send_email >> end
