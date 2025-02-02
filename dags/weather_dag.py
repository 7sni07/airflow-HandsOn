from airflow import DAG
from datetime import timedelta, datetime
import json
import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile






def kelvin_to_celsius(kelvin_temp):
    celsius_temp = kelvin_temp - 273.15
    return celsius_temp

def download_data_from_minio(bucket_name, key, local_path):
        # Get Data From Minio
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.get_key(key, bucket_name).download_file(local_path)

def load_data_to_postgres():
        df = pd.read_csv("/opt/airflow/dags/city.csv")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO city_look_up (city, state, census_2020,land_Area_sq_mile_2020) VALUES (%s, %s, %s, %s)", (row['city'], row['state'], row['census_2020'], row['land_Area_sq_mile_2020']))
        conn.commit()
        cursor.close()
        conn.close()

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_1.extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp_celsius,
                        "Minimun Temp (C)":min_temp_celsius,
                        "Maximum Temp (C)": max_temp_celsius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    df_data.to_csv("/opt/airflow/dags/current_weather_data_rabat.csv", index=False, header=False)

def load_weather():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY weather_data FROM stdin WITH DELIMITER as ','",
        filename='/opt/airflow/dags/current_weather_data_rabat.csv'
    )

def save_joined_data_to_minio(task_instance):
    data = task_instance.xcom_pull(task_ids="task_join_data")
    df_data = pd.DataFrame(data, columns = ['city', 'description', 'temperature_farenheit', 'minimun_temp_farenheit', 'maximum_temp_farenheit', 'pressure','humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time', 'state', 'census_2020', 'land_area_sq_mile_2020'])
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_weather_data_' + dt_string
    with NamedTemporaryFile(mode='w', suffix=f"{dt_string}.csv") as f:
        df_data.to_csv(f.name , index=False)
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f.name,
            bucket_name="airflow",
            key=f"weather/{dt_string}.csv",
            replace=True
        )

default_args = {
    'owner': '7sni07',
    'start_date': datetime(2024,8,6),
    'retries':2,
    'retry_delay':timedelta(minutes=2)
}

with DAG(dag_id='weather_dag_hands_on_2_v05',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
        
        start_pipeline = DummyOperator(
             task_id='task_start_pipeline'
        )

        with TaskGroup(group_id = 'group_1', tooltip= "Extract_from_minio_and_weatherapi") as group_1:
                create_table_1 = PostgresOperator(
                        task_id='task_creat_table_1',
                        postgres_conn_id = "postgres_conn",
                        sql='''
                            CREATE TABLE IF NOT EXISTS city_look_up (
                            city TEXT NOT NULL,
                            state TEXT NOT NULL,
                            census_2020 numeric NOT NULL,
                            land_Area_sq_mile_2020 numeric NOT NULL
                            );
                            '''
                )

                truncate_table = PostgresOperator(
                        task_id='task_truncate_table',
                        postgres_conn_id = "postgres_conn",
                        sql=''' TRUNCATE TABLE city_look_up;
                '''
                )

                download_data = PythonOperator(
                        task_id = 'task_download_data_from_minio',
                        python_callable = download_data_from_minio,
                                op_kwargs={
                                'bucket_name': 'airflow',
                                'key': 'city.csv',
                                'local_path': '/opt/airflow/dags/city.csv',
                            },
                )

                load_data = PythonOperator(
                       task_id = 'task_load_data_to_postgres',
                       python_callable = load_data_to_postgres
                )

                create_table_2 = PostgresOperator(
                task_id='task_create_table_2',
                postgres_conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    city TEXT,
                    description TEXT,
                    temperature_farenheit NUMERIC,
                    minimun_temp_farenheit NUMERIC,
                    maximum_temp_farenheit NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
                );
                '''
                )
                is_weather_api_ready = HttpSensor(
                task_id='is_weather_api_ready',
                http_conn_id='weathermap_api',
                endpoint='data/2.5/weather?q=Rabat&appid=6de5b50acef923b84238adb1cdd7ce78'
                )


                extract_weather_data= SimpleHttpOperator(
                task_id='extract_weather_data',
                http_conn_id='weathermap_api',
                endpoint='data/2.5/weather?q=Rabat&appid=6de5b50acef923b84238adb1cdd7ce78',
                method='GET',
                response_filter=lambda r: json.loads(r.text) if r.status_code == 200 else {},
                log_response=True
                )

                transform_load_weather_data = PythonOperator(
                task_id='transform_load_weather_data',
                python_callable=transform_load_data
                )

                load_weather_data = PythonOperator(
                task_id= 'task_load_weather_data',
                python_callable=load_weather
                )



                create_table_1 >> truncate_table >> download_data >> load_data
                create_table_2 >> is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> load_weather_data

        join_data = PostgresOperator(
                task_id='task_join_data',
                postgres_conn_id = "postgres_conn",
                sql= '''SELECT 
                    w.city,                    
                    description,
                    temperature_farenheit,
                    minimun_temp_farenheit,
                    maximum_temp_farenheit,
                    pressure,
                    humidity,
                    wind_speed,
                    time_of_record,
                    sunrise_local_time,
                    sunset_local_time,
                    state,
                    census_2020,
                    land_area_sq_mile_2020                    
                    FROM weather_data w
                    INNER JOIN city_look_up c
                        ON w.city = c.city                                      
                ;
                '''
            )

        load_joined_data = PythonOperator(
            task_id= 'task_load_joined_data',
            python_callable=save_joined_data_to_minio
            )
        
        end_pipeline = DummyOperator(
                task_id = 'task_end_pipeline'
        )
        
        start_pipeline >> group_1 >> join_data >> load_joined_data >> end_pipeline
