from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
import shutil
import os
import requests
import uuid
import time
import re

app = FastAPI()

UPLOAD_DIR = "/app/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@app.get("/", response_class=HTMLResponse)
def home():
    with open("/app/web/index.html", "r", encoding="utf-8") as file:
        return file.read()


ALLOWED_EXTENSIONS = {".csv", ".tsv", ".tab", ".txt", ".xlsx", ".xls"}


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower() if file.filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        return {
            "success": False,
            "error": f"Unsupported format '{ext or 'unknown'}'. Allowed formats: CSV (.csv), TSV (.tsv, .tab), Text (.txt), and Excel (.xlsx, .xls)"
        }

    file_path = os.path.join(UPLOAD_DIR, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Sanitize run_id to strictly match Airflow pattern: ^[A-Za-z0-9_.~:+-]+$
    safe_name = re.sub(r"[^A-Za-z0-9_.~+-]", "_", file.filename)
    run_id = f"upload_{int(time.time())}_{uuid.uuid4().hex[:6]}_{safe_name}"
    airflow_triggered = False
    airflow_msg = ""
    status_code = None

    try:
        response = requests.post(
            "http://airflow:8080/api/v1/dags/csv_cleaning_pipeline/dagRuns",
            json={
                "dag_run_id": run_id,
                "conf": {"filename": file.filename}
            },
            auth=("admin", "admin"),
            timeout=10
        )
        status_code = response.status_code
        if response.status_code in (200, 201):
            airflow_triggered = True
            airflow_msg = f"Airflow DAG triggered automatically (run ID: {run_id})"
        else:
            try:
                err_json = response.json()
                detail = err_json.get("detail") or err_json.get("title") or str(err_json)
                airflow_msg = f"Airflow API responded with {response.status_code}: {detail}"
            except Exception:
                airflow_msg = f"Airflow API responded with status {response.status_code}"
    except Exception as e:
        airflow_msg = f"Error communicating with Airflow: {str(e)}"

    return {
        "success": True,
        "filename": file.filename,
        "airflow_triggered": airflow_triggered,
        "airflow_status_code": status_code,
        "message": airflow_msg,
        "airflow_url": "http://localhost:8080/dags/csv_cleaning_pipeline/grid"
    }