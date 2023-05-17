import sqlite3
from src.server import APP
from fastapi.testclient import TestClient
from src.process_db import init_db, add_item, get_status

client = TestClient(APP)

def test_restart_status():
    #if the server restarts, the db gets wiped.  This test checks that the status query returns the correct error message
    init_db()
    response = client.get("/aragorn/asyncquery_status/defunct_job_id")
    assert response.status_code == 200
    status_response = response.json()
    assert status_response["status"] == "Failed"
    assert status_response["description"] == "No record of this job id is found, possibly due to a server restart."
    assert status_response["logs"] == []

def test_add_items():
    init_db()
    job_id = "abc123"
    add_item(job_id, "Starting job", 200)
    add_item(job_id, "Complete", 200)
    response = client.get(f"/aragorn/asyncquery_status/{job_id}")
    assert response.status_code == 200
    status_response = response.json()
    assert status_response["status"] == "Completed"
    assert status_response["description"] == "The job has completed successfully."
    assert len(status_response["logs"]) == 2
    assert status_response["logs"][0]["message"] == "Starting job"
    assert status_response["logs"][0]["level"] == "INFO"
    assert status_response["logs"][1]["message"] == "Complete"

def test_running():
    init_db()
    job_id = "abc123"
    add_item(job_id, "Starting job", 200)
    add_item(job_id, "hey doing stuff", 200)
    response = client.get(f"/aragorn/asyncquery_status/{job_id}")
    assert response.status_code == 200
    status_response = response.json()
    assert status_response["status"] == "Running"
    assert len(status_response["logs"]) == 2

def test_failed():
    init_db()
    job_id = "abc123"
    add_item(job_id, "Starting job", 200)
    add_item(job_id, "o no", 512)
    response = client.get(f"/aragorn/asyncquery_status/{job_id}")
    assert response.status_code == 200
    status_response = response.json()
    assert status_response["status"] == "Failed"
    assert len(status_response["logs"]) == 2
