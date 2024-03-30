# Libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json
import logging
import jsonschema
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# Default arguments definition for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# JSON schema definition to validate student data
schema_students = {
    "type": "object",
    "properties": {
        "students": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "student_id": {"type": "string"},
                    "name": {"type": "string"},
                    "grades": {
                        "type": "object",
                        "properties": {
                            "math": {"type": "integer"},
                            "science": {"type": "integer"},
                            "history": {"type": "integer"},
                            "english": {"type": "integer"}
                        },
                        "required": ["math", "science", "history", "english"]
                    }
                },
                "required": ["student_id", "name", "grades"]
            }
        }
    },
    "required": ["students"]
}

schema_missed_days = {
    "type": "object",
    "properties": {
        "missed_classes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "student_id": {"type": "string"},
                    "missed_days": {"type": "integer"}
                },
                "required": ["student_id", "missed_days"]
            }
        }
    },
    "required": ["missed_classes"]
}


# Function to validate data from students.json file
def validate_students():
    logging.info("Validating students.json data...")
    with open('/students.json', 'r') as file:
        students_data = json.load(file)
        try:
            jsonschema.validate(instance=students_data, schema=schema_students)
            logging.info("students.json data is valid.")
            return students_data
        except jsonschema.exceptions.ValidationError as e:
            logging.error(f"Validation error: {e}")
            raise

# Function to validate data from missed_days.json file
def validate_missed_days():
    logging.info("Validating missed_days.json data...")
    with open('/missed_days.json', 'r') as file:
        missed_days_data = json.load(file)
        try:
            jsonschema.validate(instance=missed_days_data, schema=schema_missed_days)
            logging.info("missed_days.json data is valid.")
            return missed_days_data
        except jsonschema.exceptions.ValidationError as e:
            logging.error(f"Validation error: {e}")
            raise

# Function to join datasets using DataFrame
def join_datasets(students_data, missed_days_data):
    try:
        df_students = pd.DataFrame(students_data['students'])
        df_missed_days = pd.DataFrame(missed_days_data['missed_classes'])
        df_merged = pd.merge(df_students, df_missed_days, on='student_id', how='left')
        # Handling null values resulting from the join
        df_merged['missed_days'].fillna(0, inplace=True)
        logging.info("Datasets joined successfully.")
        return df_merged
    except Exception as e:
        logging.error(f"Error joining datasets: {e}")
        raise

# Function to load data into Snowflake
def load_data_to_snowflake(df_merged):
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user='thaiscxxx',
            password='xxx*',
            account='xxx'
            #database='VIEWS_STUDENTS',
            #schema='STUDENTS_SEMANTIC'
        )
        # Load data into Snowflake
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS VIEWS_STUDENTS")
        cursor.execute("USE DATABASE VIEWS_STUDENTS")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS STUDENTS_SEMANTIC")
        cursor.execute("USE SCHEMA STUDENTS_SEMANTIC")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FINAL_MERGED (
                student_id VARCHAR,
                grades VARCHAR,
                missed_days FLOAT
            )
        """)
        # Convert DataFrame to SQL and load into final_merged table
        success, nchunks, nrows, _ = write_pandas(conn=conn
                                                ,df=df_merged
                                                ,table_name='FINAL_MERGED'
                                                ,database='VIEWS_STUDENTS'
                                                ,schema='STUDENTS_SEMANTIC'
                                                ,auto_create_table=False)
        logging.info("Data loaded into Snowflake successfully.")
    except Exception as e:
        logging.error(f"Error loading data into Snowflake: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Definition of the DAG
with DAG('migrate_student_data_to_snowflake',
         default_args=default_args,
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='validate_students',
        python_callable=validate_students
    )

    t2 = PythonOperator(
        task_id='validate_missed_days',
        python_callable=validate_missed_days
    )

    t3 = PythonOperator(
        task_id='join_datasets',
        python_callable=join_datasets,
        op_args=[t1.output, t2.output]
    )

    t4 = PythonOperator(
        task_id='load_data_to_snowflake',
        python_callable=load_data_to_snowflake,
        op_args=[t3.output]
    )

    t1 >> t2 >> t3 >> t4
