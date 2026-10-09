"""Unit tests for FastAPI web upload service."""
import io
import pytest
from fastapi.testclient import TestClient

from web.main import app, ALLOWED_EXTENSIONS
import web.main as web_module


@pytest.fixture
def client():
    return TestClient(app)


def test_allowed_extensions():
    assert ".csv" in ALLOWED_EXTENSIONS
    assert ".tsv" in ALLOWED_EXTENSIONS
    assert ".xlsx" in ALLOWED_EXTENSIONS
    assert ".xls" in ALLOWED_EXTENSIONS
    assert ".exe" not in ALLOWED_EXTENSIONS


def test_home_page_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "html" in response.headers["content-type"].lower()


def test_upload_rejected_extension(client):
    file_content = b"fake binary data"
    response = client.post(
        "/upload",
        files={"file": ("malicious.exe", io.BytesIO(file_content), "application/octet-stream")},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert "Unsupported format" in data["error"]


def test_upload_valid_file(client, tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    monkeypatch.setattr(web_module, "UPLOAD_DIR", str(upload_dir))

    # Mock requests.post to avoid actual Airflow network calls in unit tests
    class MockResponse:
        status_code = 201

        def json(self):
            return {"dag_run_id": "test_run"}

    monkeypatch.setattr(web_module.requests, "post", lambda *args, **kwargs: MockResponse())

    csv_data = b"id,name,age,email,salary\n1,Alice,25,a@b.com,100\n"
    response = client.post(
        "/upload",
        files={"file": ("dataset.csv", io.BytesIO(csv_data), "text/csv")},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["filename"] == "dataset.csv"
    assert data["airflow_triggered"] is True
    assert (upload_dir / "dataset.csv").exists()
