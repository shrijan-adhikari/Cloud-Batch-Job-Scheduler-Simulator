"""Tests for FastAPI REST endpoints, WebSocket broadcasting, and OS bridge safety."""

import pytest
from fastapi.testclient import TestClient
from app import app, engine


@pytest.fixture
def client():
    """Create a FastAPI TestClient instance."""
    with TestClient(app) as test_client:
        yield test_client


def test_get_state(client):
    """Test retrieving TickSnapshot from /api/state."""
    resp = client.get("/api/state")
    assert resp.status_code == 200
    data = resp.json()
    assert "tick" in data
    assert "cluster" in data
    assert "queues" in data
    assert "metrics" in data
    assert "host" in data
    assert data["mode"] == "simulation"


def test_get_and_update_config(client):
    """Test retrieving and updating scheduler configuration."""
    get_resp = client.get("/api/config")
    assert get_resp.status_code == 200
    cfg = get_resp.json()
    assert "schedulers" in cfg

    # Update quantum and short_term_policy
    update_resp = client.post(
        "/api/config",
        json={"time_quantum": 6, "short_term_policy": "sjf"},
    )
    assert update_resp.status_code == 200
    updated_cfg = update_resp.json()
    assert updated_cfg["schedulers"]["time_quantum"] == 6
    assert updated_cfg["schedulers"]["short_term_policy"] == "sjf"


def test_simulation_control_step_and_reset(client):
    """Test stepping and resetting simulation."""
    reset_resp = client.post("/api/control/reset")
    assert reset_resp.status_code == 200
    assert reset_resp.json()["tick"] == 0

    step_resp = client.post("/api/control/step")
    assert step_resp.status_code == 200
    assert step_resp.json()["tick"] == 0  # snapshot at tick 0 returned

    state_resp = client.get("/api/state")
    assert state_resp.json()["tick"] == 1


def test_start_pause_control(client):
    """Test starting and pausing the simulation."""
    start_resp = client.post("/api/control/start")
    assert start_resp.status_code == 200
    assert start_resp.json()["running"] is True

    pause_resp = client.post("/api/control/pause")
    assert pause_resp.status_code == 200
    assert pause_resp.json()["running"] is False


def test_sbatch_and_scancel(client):
    """Test job submission and cancellation via REST."""
    sub_resp = client.post(
        "/api/jobs/sbatch",
        json={
            "name": "test-batch-job",
            "user": "tester",
            "burst_time": 8,
            "memory_gb": 12,
            "priority": 2,
        },
    )
    assert sub_resp.status_code == 200
    data = sub_resp.json()
    job_id = data["job_id"]
    assert job_id >= 101

    # Cancel the newly submitted job
    cancel_resp = client.post(f"/api/jobs/scancel/{job_id}")
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["cancelled"] is True


def test_squeue_sinfo_sacct(client):
    """Test Slurm-style query endpoints."""
    sq_resp = client.get("/api/jobs/squeue")
    assert sq_resp.status_code == 200
    assert "jobs" in sq_resp.json()

    sinfo_resp = client.get("/api/jobs/sinfo")
    assert sinfo_resp.status_code == 200
    sinfo_data = sinfo_resp.json()
    assert "num_cpus" in sinfo_data
    assert "total_memory_gb" in sinfo_data

    sacct_resp = client.get("/api/jobs/sacct")
    assert sacct_resp.status_code == 200
    assert "jobs" in sacct_resp.json()


def test_benchmark_api(client):
    """Test remote benchmark execution endpoint."""
    resp = client.post("/api/benchmark/run")
    assert resp.status_code == 200
    data = resp.json()
    assert "benchmark" in data
    bench = data["benchmark"]
    assert set(bench.keys()) == {"fcfs", "sjf", "srtf", "round_robin", "priority"}


def test_websocket_broadcast(client):
    """Test WebSocket connection and initial snapshot broadcast."""
    with client.websocket_connect("/ws") as websocket:
        msg = websocket.receive_text()
        assert msg is not None
        import json
        snap = json.loads(msg)
        assert "tick" in snap
        assert "cluster" in snap
        assert "metrics" in snap


def test_mode_switch_safety(client):
    """Test switching between simulation and live OS modes and default safety."""
    # Verify live mode is disabled by default
    state = client.get("/api/state").json()
    assert state["mode"] == "simulation"
    assert engine.os_bridge is None

    # Switch to live mode
    mode_resp = client.post("/api/control/mode", json={"mode": "live"})
    assert mode_resp.status_code == 200
    assert mode_resp.json()["mode"] == "live"
    assert engine.os_bridge is not None

    # Switch back to simulation mode (ensuring cleanup)
    mode_resp = client.post("/api/control/mode", json={"mode": "simulation"})
    assert mode_resp.status_code == 200
    assert mode_resp.json()["mode"] == "simulation"
    assert engine.os_bridge is None
