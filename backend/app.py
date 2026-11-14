"""FastAPI application providing REST endpoints and WebSocket broadcasting for the scheduler."""

import asyncio
from contextlib import asynccontextmanager
import json
import os
from typing import Any, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from simulator.engine import Engine
from simulator.job import Job, generate_workload, clone_jobs, load_workload
from os_bridge.process_manager import ProcessManager
from os_bridge import host_monitor
from metrics.benchmark import run_benchmark


# Pydantic Schemas for Validation
class SubmitJobRequest(BaseModel):
    name: str = Field(..., description="Job identifier name")
    user: str = Field(default="user", description="Submitting username")
    burst_time: int = Field(..., ge=1, description="Burst duration in ticks")
    memory_gb: int = Field(..., ge=1, description="Memory requirement in GB")
    priority: int = Field(default=3, ge=1, le=5, description="Priority (1=highest, 5=lowest)")


class ModeRequest(BaseModel):
    mode: str = Field(..., description="'live' or 'simulation'")


class ConfigUpdateRequest(BaseModel):
    long_term_policy: Optional[str] = None
    medium_term_policy: Optional[str] = None
    short_term_policy: Optional[str] = None
    time_quantum: Optional[int] = Field(None, ge=1)
    aging_interval_ticks: Optional[int] = Field(None, ge=1)
    min_residency_ticks: Optional[int] = Field(None, ge=1)
    max_swaps_per_tick: Optional[int] = Field(None, ge=1)
    tick_duration_sec: Optional[float] = Field(None, gt=0.0)


# Global Engine and State Management
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    app_config = json.load(f)

# Concurrency and Execution State
_engine_locks: dict[Any, asyncio.Lock] = {}


def get_engine_lock() -> asyncio.Lock:
    """Return an asyncio.Lock tied to the current running event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop not in _engine_locks:
        _engine_locks[loop] = asyncio.Lock()
    return _engine_locks[loop]


connected_websockets: set[WebSocket] = set()
background_task: Optional[asyncio.Task] = None
simulation_running: bool = False
tick_duration_sec: float = app_config.get("os_bridge", {}).get("tick_duration_sec", 1.0)

# Initialize Engine with deterministic seeded workload
workload_cfg = app_config.get("workload", {})
initial_workload = generate_workload(
    workload_cfg.get("num_jobs", 20),
    workload_cfg.get("random_seed", 42),
    app_config.get("cluster", {}).get("total_memory_gb", 64),
)
engine = Engine(config=app_config, jobs=initial_workload, os_bridge=None)


async def broadcast_snapshot() -> None:
    """Broadcast the current simulation snapshot to all active WebSocket clients."""
    if not connected_websockets:
        return
    snap = engine.snapshot()
    msg = json.dumps(snap)
    disconnected: set[WebSocket] = set()
    for ws in connected_websockets:
        try:
            await ws.send_text(msg)
        except Exception:
            disconnected.add(ws)
    for dead_ws in disconnected:
        connected_websockets.discard(dead_ws)


async def simulation_loop() -> None:
    """Asynchronous loop ticking the engine periodically when in running state."""
    global simulation_running
    while simulation_running:
        try:
            async with get_engine_lock():
                if not engine.finished:
                    engine.step_tick()
                else:
                    simulation_running = False
            await broadcast_snapshot()
            await asyncio.sleep(tick_duration_sec)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle."""
    yield
    global background_task
    if background_task and not background_task.done():
        background_task.cancel()
    async with get_engine_lock():
        if engine.os_bridge:
            engine.os_bridge.terminate_all()


app = FastAPI(
    title="Cloud Batch Job Scheduler Simulator API",
    description="Three-tier Operating Systems batch scheduler simulation backend.",
    version="2.1.0",
    lifespan=lifespan,
)

# CORS setup for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# WebSocket Endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Stream live simulation updates and accept client events."""
    await websocket.accept()
    connected_websockets.add(websocket)
    try:
        # Deliver immediate state snapshot upon connecting
        async with get_engine_lock():
            snap = engine.snapshot()
        await websocket.send_text(json.dumps(snap))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_websockets.discard(websocket)
    except Exception:
        connected_websockets.discard(websocket)


# REST API Endpoints
@app.get("/api/state")
async def get_state() -> dict:
    """Return the current complete TickSnapshot of the simulation."""
    async with get_engine_lock():
        return engine.snapshot()


@app.get("/api/config")
async def get_config() -> dict:
    """Return current configuration."""
    return app_config


@app.post("/api/config")
async def update_config(req: ConfigUpdateRequest) -> dict:
    """Update scheduler and runtime configuration parameters."""
    global tick_duration_sec
    async with get_engine_lock():
        sched_cfg = app_config.setdefault("schedulers", {})
        if req.long_term_policy:
            sched_cfg["long_term_policy"] = req.long_term_policy
            engine.long_term_policy = req.long_term_policy
        if req.medium_term_policy:
            sched_cfg["medium_term_policy"] = req.medium_term_policy
            engine.medium_term_policy = req.medium_term_policy
        if req.short_term_policy:
            sched_cfg["short_term_policy"] = req.short_term_policy
            engine.short_term_policy = req.short_term_policy
        if req.time_quantum:
            sched_cfg["time_quantum"] = req.time_quantum
            engine.time_quantum = req.time_quantum
        if req.aging_interval_ticks:
            sched_cfg["aging_interval_ticks"] = req.aging_interval_ticks
            engine.aging_interval_ticks = req.aging_interval_ticks
        if req.min_residency_ticks:
            sched_cfg["min_residency_ticks"] = req.min_residency_ticks
            engine.min_residency_ticks = req.min_residency_ticks
        if req.max_swaps_per_tick:
            sched_cfg["max_swaps_per_tick"] = req.max_swaps_per_tick
            engine.max_swaps_per_tick = req.max_swaps_per_tick
        if req.tick_duration_sec:
            tick_duration_sec = req.tick_duration_sec
            app_config.setdefault("os_bridge", {})["tick_duration_sec"] = req.tick_duration_sec

    await broadcast_snapshot()
    return app_config


@app.post("/api/control/start")
async def start_simulation() -> dict:
    """Start continuous simulation execution."""
    global simulation_running, background_task
    simulation_running = True
    if background_task is None or background_task.done():
        background_task = asyncio.create_task(simulation_loop())
    await broadcast_snapshot()
    return {"running": True, "tick": engine.tick}


@app.post("/api/control/pause")
async def pause_simulation() -> dict:
    """Pause continuous simulation execution."""
    global simulation_running, background_task
    simulation_running = False
    if background_task and not background_task.done():
        background_task.cancel()
        background_task = None
    await broadcast_snapshot()
    return {"running": False, "tick": engine.tick}


@app.post("/api/control/step")
async def step_simulation() -> dict:
    """Advance the simulation by exactly one clock tick."""
    async with get_engine_lock():
        snap = engine.step_tick()
    await broadcast_snapshot()
    return snap


@app.post("/api/control/reset")
async def reset_simulation() -> dict:
    """Reset the simulation back to tick 0 with initial workload."""
    global simulation_running, background_task
    simulation_running = False
    if background_task and not background_task.done():
        background_task.cancel()
        background_task = None
    async with get_engine_lock():
        engine.reset()
        snap = engine.snapshot()
    await broadcast_snapshot()
    return snap


@app.post("/api/control/mode")
async def switch_mode(req: ModeRequest) -> dict:
    """Switch execution mode between pure simulation and live OS bridge."""
    mode = req.mode.lower()
    if mode not in ("simulation", "live"):
        raise HTTPException(status_code=400, detail="Mode must be 'simulation' or 'live'")

    async with get_engine_lock():
        if mode == "live":
            if engine.os_bridge is None:
                engine.os_bridge = ProcessManager(app_config)
            app_config.setdefault("os_bridge", {})["enabled"] = True
        else:
            if engine.os_bridge is not None:
                engine.os_bridge.terminate_all()
                engine.os_bridge = None
            app_config.setdefault("os_bridge", {})["enabled"] = False
        snap = engine.snapshot()

    await broadcast_snapshot()
    return {"mode": mode, "snapshot": snap}


@app.post("/api/jobs/sbatch")
async def sbatch(req: SubmitJobRequest) -> dict:
    """Submit a batch job (Slurm sbatch style) into the simulation."""
    async with get_engine_lock():
        job = engine.submit(
            name=req.name,
            user=req.user,
            burst_time=req.burst_time,
            memory_gb=req.memory_gb,
            priority=req.priority,
        )
    await broadcast_snapshot()
    return {
        "job_id": job.id,
        "message": f"Submitted batch job {job.id}",
        "job": job.to_dict(),
    }


@app.post("/api/jobs/scancel/{job_id}")
async def scancel(job_id: int) -> dict:
    """Cancel a queued or running job (Slurm scancel style)."""
    async with get_engine_lock():
        success = engine.cancel(job_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found or already terminated")
    await broadcast_snapshot()
    return {"job_id": job_id, "cancelled": True}


@app.get("/api/jobs/squeue")
async def squeue() -> dict:
    """List all currently active jobs in NEW, READY, RUNNING, or SUSPENDED states."""
    async with get_engine_lock():
        active = (
            engine.new_queue
            + engine.ready_queue
            + engine.running_jobs
            + engine.suspended_queue
        )
        return {"jobs": [j.to_dict() for j in active]}


@app.get("/api/jobs/sinfo")
async def sinfo() -> dict:
    """Report cluster nodes, core allocation, and memory availability."""
    async with get_engine_lock():
        return {
            "num_cpus": engine.cluster.num_cpus,
            "cores": list(engine.cluster.cores),
            "total_memory_gb": engine.cluster.total_memory_gb,
            "used_memory_gb": engine.cluster.used_memory_gb,
            "available_memory_gb": engine.cluster.available_memory_gb,
            "memory_utilization": engine.cluster.memory_utilization,
            "suspension_threshold": engine.cluster.suspension_threshold,
            "resume_threshold": engine.cluster.resume_threshold,
        }


@app.get("/api/jobs/sacct")
async def sacct() -> dict:
    """Retrieve accounting and telemetry data for all jobs past and present."""
    async with get_engine_lock():
        return {"jobs": [j.to_dict() for j in engine.all_jobs]}


@app.post("/api/benchmark/run")
async def api_run_benchmark() -> dict:
    """Execute benchmark over all 5 short-term policies on an identical workload."""
    results = run_benchmark(app_config)
    return {"benchmark": results}


@app.get("/api/telemetry")
async def telemetry() -> dict:
    """Return host hardware metrics and active cluster state."""
    async with get_engine_lock():
        return {
            "host": host_monitor.sample(),
            "tick": engine.tick,
            "used_memory_gb": engine.cluster.used_memory_gb,
            "cores": list(engine.cluster.cores),
            "finished": engine.finished,
        }
