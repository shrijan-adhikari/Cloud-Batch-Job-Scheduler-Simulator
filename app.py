"""FastAPI application providing REST endpoints, WebSockets, and Control Plane backend."""

import asyncio
import json
import os
from typing import Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from simulator.job import Job, JobState, generate_workload, clone_jobs
from simulator.engine import Engine
from os_bridge.process_manager import ProcessManager
from metrics.benchmark import run_benchmark


# Pydantic Schemas
class StartRequest(BaseModel):
    tick_duration_sec: float = Field(default=1.0, ge=0.01, le=10.0)


class StepRequest(BaseModel):
    ticks: int = Field(default=1, ge=1, le=1000)


class ModeRequest(BaseModel):
    os_bridge_enabled: bool


class ResetRequest(BaseModel):
    num_cpus: Optional[int] = Field(default=None, ge=1, le=32)
    total_memory_gb: Optional[int] = Field(default=None, ge=4, le=1024)
    long_term_policy: Optional[str] = None
    medium_term_policy: Optional[str] = None
    short_term_policy: Optional[str] = None
    time_quantum: Optional[int] = Field(default=None, ge=1, le=64)
    num_jobs: Optional[int] = Field(default=None, ge=1, le=200)
    random_seed: Optional[int] = None
    os_bridge_enabled: Optional[bool] = None


class SubmitJobRequest(BaseModel):
    name: str = "batch-job"
    user: str = "alice"
    burst_time: int = Field(default=6, ge=1)
    memory_gb: int = Field(default=8, ge=1)
    priority: int = Field(default=3, ge=1, le=5)


class BenchmarkRequest(BaseModel):
    num_jobs: int = Field(default=30, ge=1, le=100)
    seed: int = Field(default=42)


# App State Container
class AppState:
    def __init__(self) -> None:
        self.config: dict = {}
        self.engine: Optional[Engine] = None
        self.process_manager: Optional[ProcessManager] = None
        self.lock = asyncio.Lock()
        self.is_running: bool = False
        self.tick_duration_sec: float = 1.0
        self.active_websockets: set[WebSocket] = set()
        self.tick_task: Optional[asyncio.Task] = None


state = AppState()


def load_app_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "cluster": {"num_cpus": 4, "total_memory_gb": 64, "memory_suspension_threshold": 0.85, "memory_resume_threshold": 0.70},
        "schedulers": {"long_term_policy": "memory_aware_fcfs", "medium_term_policy": "suspend_lowest_priority", "short_term_policy": "round_robin", "time_quantum": 4, "aging_interval_ticks": 6, "min_residency_ticks": 3, "max_swaps_per_tick": 1},
        "os_bridge": {"enabled": False, "ram_scale_mb_per_gb": 15, "tick_duration_sec": 1.0, "max_total_real_ram_mb": 1024, "max_live_workers": 16},
        "workload": {"num_jobs": 20, "random_seed": 42},
        "server": {"host": "127.0.0.1", "port": 8000},
    }


async def broadcast_snapshot(snapshot: dict) -> None:
    """Broadcast JSON snapshot to all connected WebSocket clients."""
    if not state.active_websockets:
        return
    dead = set()
    for ws in list(state.active_websockets):
        try:
            await ws.send_json(snapshot)
        except Exception:
            dead.add(ws)
    state.active_websockets.difference_update(dead)


async def background_tick_loop() -> None:
    """Async background loop stepping the engine while running."""
    while True:
        try:
            if state.is_running and state.engine is not None:
                async with state.lock:
                    if not state.engine.finished:
                        snap = state.engine.step_tick()
                    else:
                        state.is_running = False
                        snap = state.engine.snapshot()
                await broadcast_snapshot(snap)
            await asyncio.sleep(state.tick_duration_sec)
        except asyncio.CancelledError:
            break
        except Exception as e:
            import traceback
            traceback.print_exc()
            await asyncio.sleep(state.tick_duration_sec)



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Engine & ProcessManager
    state.config = load_app_config()
    state.process_manager = ProcessManager(state.config)
    os_enabled = state.config.get("os_bridge", {}).get("enabled", False)
    state.process_manager.enabled = os_enabled

    w_cfg = state.config.get("workload", {})
    initial_jobs = generate_workload(
        num_jobs=w_cfg.get("num_jobs", 20),
        seed=w_cfg.get("random_seed", 42),
        total_memory_gb=state.config.get("cluster", {}).get("total_memory_gb", 64),
    )
    bridge_to_use = state.process_manager if os_enabled else None
    state.engine = Engine(config=state.config, jobs=initial_jobs, os_bridge=bridge_to_use)
    state.tick_duration_sec = state.config.get("os_bridge", {}).get("tick_duration_sec", 1.0)

    # Start tick loop
    state.tick_task = asyncio.create_task(background_tick_loop())

    yield

    # Shutdown cleanup
    if state.tick_task:
        state.tick_task.cancel()
    if state.process_manager:
        state.process_manager.terminate_all()


app = FastAPI(title="Cloud Batch Job Scheduler Simulator", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

dashboard_dir = os.path.join(os.path.dirname(__file__), "dashboard")
if os.path.exists(dashboard_dir):
    app.mount("/static", StaticFiles(directory=dashboard_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> Any:
    index_file = os.path.join(dashboard_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return HTMLResponse("<h2>Dashboard index.html not found</h2>", status_code=404)


# 1. Cluster & Simulation Control
@app.get("/api/state")
async def get_state() -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        return state.engine.snapshot()


@app.post("/api/control/start")
async def start_simulation(req: Optional[StartRequest] = None) -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    if req and req.tick_duration_sec:
        state.tick_duration_sec = req.tick_duration_sec
    state.is_running = True
    async with state.lock:
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"status": "running", "tick": state.engine.tick, "snapshot": snap}


@app.post("/api/control/pause")
async def pause_simulation() -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    state.is_running = False
    async with state.lock:
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"status": "paused", "tick": state.engine.tick, "snapshot": snap}



@app.post("/api/control/step")
async def step_simulation(req: Optional[StepRequest] = None) -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    ticks = req.ticks if req else 1
    async with state.lock:
        for _ in range(ticks):
            if not state.engine.finished:
                state.engine.step_tick()
            else:
                break
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return snap


@app.post("/api/control/mode")
async def toggle_mode(req: ModeRequest) -> dict:
    if state.engine is None or state.process_manager is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        state.process_manager.enabled = req.os_bridge_enabled
        if req.os_bridge_enabled:
            state.engine.os_bridge = state.process_manager
            # Sync running jobs to real processes
            for job in state.engine.running_jobs:
                if job.assigned_cpu is not None and job.os_pid is None:
                    state.process_manager.start_or_resume(job, job.assigned_cpu)
        else:
            state.engine.os_bridge = None
            state.process_manager.terminate_all()
            for job in state.engine.all_jobs:
                job.os_pid = None
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"os_bridge_enabled": req.os_bridge_enabled}


@app.post("/api/control/reset")
async def reset_simulation(req: Optional[ResetRequest] = None) -> dict:
    if state.engine is None or state.process_manager is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        state.is_running = False
        state.process_manager.terminate_all()

        if req:
            cfg = state.engine.config
            if req.num_cpus is not None:
                cfg["cluster"]["num_cpus"] = req.num_cpus
                state.process_manager.num_cpus = req.num_cpus
            if req.total_memory_gb is not None:
                cfg["cluster"]["total_memory_gb"] = req.total_memory_gb
            if req.long_term_policy is not None:
                cfg["schedulers"]["long_term_policy"] = req.long_term_policy
            if req.medium_term_policy is not None:
                cfg["schedulers"]["medium_term_policy"] = req.medium_term_policy
            if req.short_term_policy is not None:
                cfg["schedulers"]["short_term_policy"] = req.short_term_policy
            if req.time_quantum is not None:
                cfg["schedulers"]["time_quantum"] = req.time_quantum
            if req.os_bridge_enabled is not None:
                cfg["os_bridge"]["enabled"] = req.os_bridge_enabled
                state.process_manager.enabled = req.os_bridge_enabled

            num_j = req.num_jobs or cfg.get("workload", {}).get("num_jobs", 20)
            seed = req.random_seed if req.random_seed is not None else cfg.get("workload", {}).get("random_seed", 42)
            fresh_jobs = generate_workload(num_j, seed, cfg["cluster"]["total_memory_gb"])
            bridge = state.process_manager if state.process_manager.enabled else None
            state.engine = Engine(config=cfg, jobs=fresh_jobs, os_bridge=bridge)
        else:
            state.engine.reset()

        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return snap


# 2. SLURM Job & Node Operations
@app.post("/api/jobs")
async def submit_job(req: SubmitJobRequest) -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        job = state.engine.submit(
            name=req.name,
            user=req.user,
            burst_time=req.burst_time,
            memory_gb=req.memory_gb,
            priority=req.priority,
        )
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"submitted": job.to_dict(), "message": f"Submitted batch job {job.id}"}


@app.delete("/api/jobs/{job_id}")
async def cancel_job(job_id: int) -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        ok = state.engine.cancel(job_id)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found or already terminated")
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"cancelled": True, "job_id": job_id}


@app.post("/api/jobs/{job_id}/suspend")
async def suspend_job_manual(job_id: int) -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        target = next((j for j in state.engine.all_jobs if j.id == job_id), None)
        if not target or target.state in (JobState.SUSPENDED, JobState.TERMINATED):
            raise HTTPException(status_code=400, detail="Job cannot be suspended")

        if target in state.engine.running_jobs:
            state.engine.running_jobs.remove(target)
            if target.assigned_cpu is not None:
                state.engine.cluster.vacate(target.assigned_cpu)
                target.assigned_cpu = None
            state.engine.cluster.release(target)
        elif target in state.engine.ready_queue:
            state.engine.ready_queue.remove(target)
            state.engine.cluster.release(target)

        target.state = JobState.SUSPENDED
        target.quantum_used = 0
        target.age_counter = 0
        target.suspension_count += 1
        state.engine.suspended_queue.append(target)
        if state.engine.os_bridge:
            state.engine.os_bridge.suspend(target)
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"status": "SUSPENDED", "job_id": job_id}


@app.post("/api/jobs/{job_id}/resume")
async def resume_job_manual(job_id: int) -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        target = next((j for j in state.engine.suspended_queue if j.id == job_id), None)
        if not target:
            raise HTTPException(status_code=400, detail="Job not found in suspended queue")
        if not state.engine.cluster.can_fit(target):
            raise HTTPException(status_code=400, detail="Insufficient RAM to resume job")

        state.engine.suspended_queue.remove(target)
        state.engine.cluster.allocate(target)
        target.state = JobState.READY
        target.last_resumed_time = state.engine.tick
        target.age_counter = 0
        state.engine.ready_queue.append(target)
        snap = state.engine.snapshot()
    await broadcast_snapshot(snap)
    return {"status": "READY", "job_id": job_id}


@app.get("/api/squeue")
async def get_squeue() -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        active_jobs = [
            j.to_dict()
            for j in state.engine.all_jobs
            if j.state in (JobState.NEW, JobState.READY, JobState.RUNNING, JobState.SUSPENDED)
        ]
        return {"jobs": active_jobs}


@app.get("/api/sinfo")
async def get_sinfo() -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        c = state.engine.cluster
        running_cnt = len(state.engine.running_jobs)
        free_cores = len(c.free_cores())
        live_os = state.engine.os_bridge is not None
        if live_os and state.process_manager:
            live_status = f"LIVE ({sum(1 for w in state.process_manager.workers.values() if w['status']=='running')} active, {sum(1 for w in state.process_manager.workers.values() if w['status']=='suspended')} susp)"
        else:
            live_status = "DISABLED"

        return {
            "partition": "batch*",
            "avail": "up",
            "timelimit": "infinite",
            "cpus": {
                "allocated": running_cnt,
                "idle": free_cores,
                "total": c.num_cpus,
            },
            "memory_gb": {
                "used": c.used_memory_gb,
                "total": c.total_memory_gb,
                "utilization_percent": round(c.memory_utilization * 100.0, 1),
            },
            "state": "allocated" if running_cnt > 0 else "idle",
            "os_bridge": live_status,
        }


@app.get("/api/sacct")
async def get_sacct() -> dict:
    if state.engine is None:
        raise HTTPException(status_code=500, detail="Engine uninitialized")
    async with state.lock:
        return {"jobs": [j.to_dict() for j in state.engine.all_jobs]}


# 3. Policy Benchmark Endpoint
@app.post("/api/benchmark")
async def benchmark_endpoint(req: Optional[BenchmarkRequest] = None) -> dict:
    num_j = req.num_jobs if req else 30
    seed = req.seed if req else 42
    total_mem = state.engine.cluster.total_memory_gb if state.engine else 64
    b_jobs = generate_workload(num_j, seed, total_mem)
    cfg = state.engine.config if state.engine else load_app_config()

    res = run_benchmark(cfg, b_jobs)
    return {
        "num_jobs": num_j,
        "seed": seed,
        "policies": res,
    }


# 4. WebSocket Loop
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    state.active_websockets.add(ws)
    try:
        if state.engine:
            async with state.lock:
                await ws.send_json(state.engine.snapshot())
        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")
                if action == "start":
                    await start_simulation()
                elif action == "pause":
                    await pause_simulation()
                elif action == "step":
                    await step_simulation()
                elif action == "reset":
                    await reset_simulation()
            except Exception:
                pass
    except WebSocketDisconnect:
        state.active_websockets.discard(ws)
    except Exception:
        state.active_websockets.discard(ws)
