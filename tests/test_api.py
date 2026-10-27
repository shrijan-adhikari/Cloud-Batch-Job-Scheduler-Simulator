"""Test suite for FastAPI REST endpoints and WebSockets in app.py."""

import pytest
from fastapi.testclient import TestClient
from app import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_get_state(client: TestClient) -> None:
    """Verify GET /api/state returns valid TickSnapshot."""
    res = client.get("/api/state")
    assert res.status_code == 200
    data = res.json()
    assert "tick" in data
    assert "cluster" in data
    assert "queues" in data
    assert "jobs" in data
    assert "metrics" in data
    assert "host" in data
    assert "config" in data


def test_control_step_and_pause_start(client: TestClient) -> None:
    """Verify step, start, pause control endpoints."""
    # Step 1 tick
    res = client.post("/api/control/step", json={"ticks": 1})
    assert res.status_code == 200
    snap = res.json()
    assert snap["tick"] >= 1

    # Start loop
    res = client.post("/api/control/start", json={"tick_duration_sec": 1.0})
    assert res.status_code == 200
    assert res.json()["status"] == "running"

    # Pause loop
    res = client.post("/api/control/pause")
    assert res.status_code == 200
    assert res.json()["status"] == "paused"


def test_sbatch_squeue_sacct_scancel(client: TestClient) -> None:
    """Verify SLURM job submission, queue inspection, accounting, and cancellation."""
    # 1. sbatch (POST /api/jobs)
    payload = {
        "name": "ai-train",
        "user": "alice",
        "burst_time": 6,
        "memory_gb": 8,
        "priority": 1,
    }
    res = client.post("/api/jobs", json=payload)
    assert res.status_code == 200
    sub = res.json()
    assert "submitted" in sub
    job_id = sub["submitted"]["id"]
    assert sub["message"] == f"Submitted batch job {job_id}"

    # 2. squeue (GET /api/squeue)
    res = client.get("/api/squeue")
    assert res.status_code == 200
    active = res.json().get("jobs", [])
    assert any(j["id"] == job_id for j in active)

    # 3. sacct (GET /api/sacct)
    res = client.get("/api/sacct")
    assert res.status_code == 200
    acct = res.json().get("jobs", [])
    assert any(j["id"] == job_id for j in acct)

    # 4. scancel (DELETE /api/jobs/{id})
    res = client.delete(f"/api/jobs/{job_id}")
    assert res.status_code == 200
    assert res.json()["cancelled"] is True
    assert res.json()["job_id"] == job_id


def test_sinfo(client: TestClient) -> None:
    """Verify GET /api/sinfo returns SLURM partition summary."""
    res = client.get("/api/sinfo")
    assert res.status_code == 200
    info = res.json()
    assert info["partition"] == "batch*"
    assert "cpus" in info
    assert "memory_gb" in info
    assert "os_bridge" in info


def test_mode_toggle(client: TestClient) -> None:
    """Verify toggling Live OS execution mode."""
    res = client.post("/api/control/mode", json={"os_bridge_enabled": False})
    assert res.status_code == 200
    assert res.json()["os_bridge_enabled"] is False

    res = client.post("/api/control/mode", json={"os_bridge_enabled": True})
    assert res.status_code == 200
    assert res.json()["os_bridge_enabled"] is True

    # Revert to False for unit test safety
    client.post("/api/control/mode", json={"os_bridge_enabled": False})


def test_benchmark_endpoint(client: TestClient) -> None:
    """Verify POST /api/benchmark runs 5 policies and returns results."""
    res = client.post("/api/benchmark", json={"num_jobs": 10, "seed": 42})
    assert res.status_code == 200
    data = res.json()
    assert data["num_jobs"] == 10
    assert "policies" in data
    assert set(data["policies"].keys()) == {"fcfs", "sjf", "srtf", "round_robin", "priority"}


def test_control_reset(client: TestClient) -> None:
    """Verify POST /api/control/reset rebuilds engine state."""
    res = client.post(
        "/api/control/reset",
        json={
            "num_cpus": 4,
            "total_memory_gb": 64,
            "short_term_policy": "round_robin",
            "time_quantum": 4,
            "num_jobs": 15,
            "random_seed": 99,
        },
    )
    assert res.status_code == 200
    snap = res.json()
    assert snap["tick"] == 0
    assert snap["cluster"]["num_cpus"] == 4
    assert snap["cluster"]["total_memory_gb"] == 64


def test_websocket_connection(client: TestClient) -> None:
    """Verify WebSocket /ws connection and initial snapshot reception."""
    with client.websocket_connect("/ws") as ws:
        snap = ws.receive_json()
        assert "tick" in snap
        assert "cluster" in snap
