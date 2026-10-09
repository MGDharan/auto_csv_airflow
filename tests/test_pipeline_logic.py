"""Unit tests for data reading, cleaning transformations, and schema validation."""
import os
import pandas as pd
import pytest

from dags.csv_cleaning_pipeline import (
    get_latest_file,
    read_data_file,
    ALLOWED_EXTENSIONS,
)
import dags.csv_cleaning_pipeline as pipeline


class MockTI:
    def __init__(self):
        self.storage = {}

    def xcom_push(self, key, value):
        self.storage[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self.storage.get(key)


def test_get_latest_file(tmp_path):
    # Empty directory
    assert get_latest_file(str(tmp_path)) is None

    # Directory with multiple files
    file1 = tmp_path / "file1.csv"
    file1.write_text("id,name\n1,A\n", encoding="utf-8")

    file2 = tmp_path / "file2.csv"
    file2.write_text("id,name\n2,B\n", encoding="utf-8")

    latest = get_latest_file(str(tmp_path))
    assert latest in ["file1.csv", "file2.csv"]

    # File with disallowed extension should be ignored
    ignored = tmp_path / "notes.doc"
    ignored.write_text("notes", encoding="utf-8")
    assert get_latest_file(str(tmp_path)) in ["file1.csv", "file2.csv"]


def test_read_data_file_csv(tmp_path):
    csv_file = tmp_path / "users.csv"
    csv_file.write_text("id,name,age\n101,John,30\n102,Jane,25\n", encoding="utf-8")

    df = read_data_file(str(csv_file))
    assert len(df) == 2
    assert list(df.columns) == ["id", "name", "age"]
    assert df.iloc[0]["name"] == "John"


def test_read_data_file_tsv(tmp_path):
    tsv_file = tmp_path / "users.tsv"
    tsv_file.write_text("id\tname\tage\n201\tAlice\t28\n", encoding="utf-8")

    df = read_data_file(str(tsv_file))
    assert len(df) == 1
    assert "name" in df.columns
    assert df.iloc[0]["name"] == "Alice"


def test_clean_csv_logic(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cleaned_dir = tmp_path / "cleaned"
    upload_dir.mkdir()
    cleaned_dir.mkdir()

    monkeypatch.setattr(pipeline, "UPLOAD_DIR", str(upload_dir))
    monkeypatch.setattr(pipeline, "CLEANED_DIR", str(cleaned_dir))

    # Create raw CSV with:
    # 1. Trailing spaces and mixed-case columns
    # 2. Unwanted column
    # 3. Duplicate row
    # 4. Out-of-bounds age (>100 or <0)
    # 5. Negative salary
    raw_data = """  ID  , Name , AGE , Email , Salary , unwanted_column
1, Alice, 25, alice@example.com, 50000, junk
1, Alice, 25, alice@example.com, 50000, junk
2, Bob, 150, bob@example.com, -1000, junk
3, Charlie, -5, charlie@example.com, 60000, junk
"""
    raw_file = upload_dir / "raw_data.csv"
    raw_file.write_text(raw_data, encoding="utf-8")

    ti = MockTI()
    context = {"ti": ti}

    output_filename = pipeline.clean_csv(**context)
    assert output_filename == "clean_raw_data.csv"
    assert ti.xcom_pull(key="cleaned_filename") == "clean_raw_data.csv"

    cleaned_file = cleaned_dir / output_filename
    assert cleaned_file.exists()

    df_cleaned = pd.read_csv(str(cleaned_file))

    # Verify duplicate dropped: originally 4 rows, 1 duplicate -> 3 rows
    assert len(df_cleaned) == 3

    # Verify unwanted_column removed
    assert "unwanted_column" not in df_cleaned.columns

    # Verify column names trimmed and lowercased
    assert set(df_cleaned.columns) == {"id", "name", "age", "email", "salary"}

    # Verify invalid age was converted to NaN
    bob_age = df_cleaned.loc[df_cleaned["id"] == 2, "age"].values[0]
    charlie_age = df_cleaned.loc[df_cleaned["id"] == 3, "age"].values[0]
    assert pd.isna(bob_age)
    assert pd.isna(charlie_age)

    # Verify negative salary was converted to NaN
    bob_salary = df_cleaned.loc[df_cleaned["id"] == 2, "salary"].values[0]
    assert pd.isna(bob_salary)


def test_validate_data_success(tmp_path, monkeypatch):
    cleaned_dir = tmp_path / "cleaned"
    cleaned_dir.mkdir()
    monkeypatch.setattr(pipeline, "CLEANED_DIR", str(cleaned_dir))

    clean_file = cleaned_dir / "clean_sample.csv"
    clean_file.write_text("id,name,age,email,salary\n1,Alice,30,alice@test.com,5000\n", encoding="utf-8")

    ti = MockTI()
    ti.xcom_push("cleaned_filename", "clean_sample.csv")
    context = {"ti": ti}

    # Should not raise exception
    pipeline.validate_data(**context)


def test_validate_data_missing_column_raises(tmp_path, monkeypatch):
    cleaned_dir = tmp_path / "cleaned"
    cleaned_dir.mkdir()
    monkeypatch.setattr(pipeline, "CLEANED_DIR", str(cleaned_dir))

    clean_file = cleaned_dir / "clean_sample.csv"
    # Missing required 'salary' column
    clean_file.write_text("id,name,age,email\n1,Alice,30,alice@test.com\n", encoding="utf-8")

    ti = MockTI()
    ti.xcom_push("cleaned_filename", "clean_sample.csv")
    context = {"ti": ti}

    with pytest.raises(Exception) as exc_info:
        pipeline.validate_data(**context)

    assert "Missing required column: 'salary'" in str(exc_info.value)
