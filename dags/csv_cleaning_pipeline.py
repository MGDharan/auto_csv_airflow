from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import pandas as pd
import numpy as np
import os
import mysql.connector

UPLOAD_DIR = "/opt/airflow/uploads"
CLEANED_DIR = "/opt/airflow/cleaned"
ALLOWED_EXTENSIONS = (".csv", ".tsv", ".tab", ".txt", ".xlsx", ".xls")


def get_latest_file(directory, extensions=ALLOWED_EXTENSIONS):
    if not os.path.exists(directory):
        return None
    files = [
        f for f in os.listdir(directory)
        if any(f.lower().endswith(ext) for ext in extensions) and not f.startswith(".")
    ]
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(os.path.join(directory, f)))
    return files[-1]


def read_data_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    print(f"Reading file '{file_path}' with detected format '{ext}'")

    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)

    # For text-based tabular formats, determine preferred delimiter
    preferred_sep = "\t" if ext in [".tsv", ".tab"] else ("," if ext == ".csv" else None)

    df = None
    for encoding in ["utf-8", "utf-8-sig", "latin1"]:
        try:
            if preferred_sep:
                df = pd.read_csv(file_path, sep=preferred_sep, encoding=encoding)
            else:
                df = pd.read_csv(file_path, sep=None, engine="python", encoding=encoding)

            # Detect if columns collapsed into a single column due to extension/delimiter mismatch
            if len(df.columns) == 1:
                first_col = str(df.columns[0])
                if any(delim in first_col for delim in [",", "\t", ";", "|"]):
                    print("Single collapsed column detected. Re-parsing with automatic delimiter detection.")
                    df = pd.read_csv(file_path, sep=None, engine="python", encoding=encoding)

            break
        except UnicodeDecodeError:
            continue
        except Exception:
            try:
                df = pd.read_csv(file_path, sep=None, engine="python", encoding=encoding)
                break
            except Exception:
                continue

    if df is None:
        try:
            df = pd.read_csv(file_path, sep=None, engine="python")
        except Exception:
            df = pd.read_csv(file_path)

    return df


def clean_csv(**context):
    dag_run = context.get("dag_run")
    filename = None
    if dag_run and dag_run.conf and "filename" in dag_run.conf:
        target = dag_run.conf["filename"]
        if os.path.exists(os.path.join(UPLOAD_DIR, target)):
            filename = target

    if not filename:
        filename = get_latest_file(UPLOAD_DIR)

    if not filename:
        raise Exception("No input data file found in uploads directory")

    input_file = os.path.join(UPLOAD_DIR, filename)
    df = read_data_file(input_file)

    print(f"Processing file: {filename}")
    print("Original rows:", len(df))
    print("Original columns:", df.columns.tolist())

    # Normalize column names: strip spaces and convert to lowercase
    df.columns = [str(col).strip().lower() for col in df.columns]

    # Remove unwanted columns
    unwanted_columns = ["unwanted_column"]
    for column in unwanted_columns:
        if column in df.columns:
            df.drop(column, axis=1, inplace=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Handle missing values in text/object columns
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].fillna("Unknown")

    # Convert numeric columns
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")
        df.loc[(df["age"] < 0) | (df["age"] > 100), "age"] = np.nan

    if "salary" in df.columns:
        df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
        df.loc[df["salary"] < 0, "salary"] = np.nan

    # Create cleaned directory
    os.makedirs(CLEANED_DIR, exist_ok=True)
    base_name = os.path.splitext(filename)[0]
    output_filename = f"clean_{base_name}.csv"
    output_file = os.path.join(CLEANED_DIR, output_filename)

    df.to_csv(output_file, index=False)
    print("Clean CSV created:", output_file)
    print("Final rows:", len(df))

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="cleaned_filename", value=output_filename)

    return output_filename


def validate_data(**context):
    ti = context.get("ti")
    cleaned_filename = None
    if ti:
        cleaned_filename = ti.xcom_pull(task_ids="clean_csv", key="cleaned_filename")

    if not cleaned_filename:
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "filename" in dag_run.conf:
            base_name = os.path.splitext(dag_run.conf["filename"])[0]
            candidate = f"clean_{base_name}.csv"
            if os.path.exists(os.path.join(CLEANED_DIR, candidate)):
                cleaned_filename = candidate

    if not cleaned_filename:
        cleaned_filename = get_latest_file(CLEANED_DIR, extensions=(".csv",))

    if not cleaned_filename:
        raise Exception("Cleaned file not found in cleaned directory")

    file_path = os.path.join(CLEANED_DIR, cleaned_filename)
    df = pd.read_csv(file_path)

    required_columns = ["id", "name", "age", "email", "salary"]
    for column in required_columns:
        if column not in df.columns:
            raise Exception(f"Missing required column: '{column}'. Available columns: {list(df.columns)}")

    print(f"Validation successful for {cleaned_filename}. Validated columns: {list(df.columns)}")


def load_mysql(**context):
    ti = context.get("ti")
    cleaned_filename = None
    if ti:
        cleaned_filename = ti.xcom_pull(task_ids="clean_csv", key="cleaned_filename")

    if not cleaned_filename:
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "filename" in dag_run.conf:
            base_name = os.path.splitext(dag_run.conf["filename"])[0]
            candidate = f"clean_{base_name}.csv"
            if os.path.exists(os.path.join(CLEANED_DIR, candidate)):
                cleaned_filename = candidate

    if not cleaned_filename:
        cleaned_filename = get_latest_file(CLEANED_DIR, extensions=(".csv",))

    if not cleaned_filename:
        raise Exception("Cleaned file not found in cleaned directory")

    file_path = os.path.join(CLEANED_DIR, cleaned_filename)
    df = pd.read_csv(file_path)

    connection = mysql.connector.connect(
        host="mysql",
        user="etluser",
        password="etlpassword",
        database="etl_db"
    )

    cursor = connection.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cleaned_sales (
            id INT,
            name VARCHAR(100),
            age INT,
            email VARCHAR(150),
            salary DECIMAL(10,2)
        )
    """)

    for _, row in df.iterrows():
        id_val = None if pd.isna(row.get("id")) else int(row["id"])
        name_val = None if pd.isna(row.get("name")) else str(row["name"])
        age_val = None if pd.isna(row.get("age")) else int(float(row["age"]))
        email_val = None if pd.isna(row.get("email")) else str(row["email"])
        salary_val = None if pd.isna(row.get("salary")) else float(row["salary"])

        cursor.execute(
            """
            INSERT INTO cleaned_sales
            (id, name, age, email, salary)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (id_val, name_val, age_val, email_val, salary_val)
        )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Successfully loaded {len(df)} rows from {cleaned_filename} into MySQL cleaned_sales table")


with DAG(
    dag_id="csv_cleaning_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    clean = PythonOperator(
        task_id="clean_csv",
        python_callable=clean_csv
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    load = PythonOperator(
        task_id="load_mysql",
        python_callable=load_mysql
    )

    clean >> validate >> load