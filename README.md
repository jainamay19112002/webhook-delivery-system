# ⚡ Reliable Webhook Delivery Engine

![CI Workflow](https://github.com/YOUR_GITHUB_USERNAME/webhook-delivery-system/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.141+-009688?style=flat&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Streams-DC382D?style=flat&logo=redis&logoColor=white)
![React](https://img.shields.io/badge/React-18+-61DAFB?style=flat&logo=react&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green.style=flat)

A high-reliability, fault-tolerant **Webhook Delivery Engine** built with **FastAPI**, **Redis Streams**, **SQLModel/SQLite**, and **React (Next.js/Vite)** with real-time **Server-Sent Events (SSE)** tracking.

Designed to demonstrate production-grade distributed system reliability patterns for senior backend engineering (SDE) interviews.

---

## 🏗️ Architecture & Data Flow

```
                      ┌─────────────────────────────────┐
                      │    Client / Ingestion API       │
                      │    (POST /api/v1/events)        │
                      └────────────────┬────────────────┘
                                       │
                                       │ (Idempotency Key Check)
                                       ▼
                     ┌───────────────────────────────────┐
                     │   Redis Stream ("webhook:events") │
                     └─────────────────┬─────────────────┘
                                       │
                                       │ (Consumer Group: "delivery_workers")
                                       ▼
                     ┌───────────────────────────────────┐
                     │    Webhook Worker Pool            │
                     │    - HMAC-SHA256 Signatures       │
                     │    - Exponential Backoff + Jitter │
                     │    - Circuit Breakers (per tenant)│
                     └─────────┬──────────────┬──────────┘
                               │              │
                   (Success /  │              │ (Max Retries Failed)
                   Failure Log)│              ▼
                               │   ┌──────────────────────────┐
                               │   │  Dead Letter Queue (DLQ) │
                               │   │  ("webhook:dlq")         │
                               │   └──────────────┬───────────┘
                               │                  │ (Manual Replay)
                               ▼                  │
                     ┌────────────────────────────▼──────┐
                     │    SQLite / Postgres Audit DB     │
                     └─────────────────┬─────────────────┘
                                       │
                                       ▼
                     ┌───────────────────────────────────┐
                     │   FastAPI SSE Real-time Stream    │
                     │   (GET /api/v1/events/stream)     │
                     └─────────────────┬─────────────────┘
                                       │
                                       ▼
                     ┌───────────────────────────────────┐
                     │    Next.js Real-time Dashboard    │
                     └───────────────────────────────────┘
```

```mermaid
graph TD
    A[Client API] -->|POST /api/v1/events| B(FastAPI Ingestion Engine)
    B -->|Check X-Idempotency-Key| C{Is Duplicate?}
    C -->|Yes| D[Return 202 Idempotent Duplicate]
    C -->|No| E[XADD webhook:events]
    E --> F[Redis Streams Consumer Group]
    F --> G[Async Worker Pool]
    G --> H[Check Circuit Breaker State]
    H -->|OPEN| I[Fast Fail & Log Attempt]
    H -->|CLOSED / HALF_OPEN| J[Compute HMAC-SHA256 & POST Webhook]
    J -->|HTTP 200 OK| K[XACK & Reset Circuit Breaker]
    J -->|HTTP 500 / Timeout| L[Exponential Backoff + Jitter Retry]
    L -->|Max Retries Exhausted| M[Push to DLQ webhook:dlq]
    G --> N[(SQLite Audit Log)]
    N --> O[FastAPI SSE Stream]
    O --> P[React Real-time Dashboard]
```

---

## 🎯 Key Reliability Features

1. **At-Least-Once Delivery Guarantee**: Events are ingested asynchronously via Redis Streams consumer groups (`XREADGROUP` / `XACK`).
2. **Idempotency Deduplication**: Ingestion API validates `X-Idempotency-Key` headers to prevent duplicate processing.
3. **Exponential Backoff with Full Jitter**: Retries failed endpoint deliveries using full jitter to eliminate thundering herd problems.
4. **Per-Subscriber Circuit Breaker**: 3-state machine (`CLOSED` → `OPEN` → `HALF_OPEN`) isolates failing endpoints and prevents hammering degraded subscriber servers.
5. **HMAC SHA-256 Signatures**: Generates `X-Webhook-Signature: t=<timestamp>,v1=<signature>` for recipient authentication & replay protection.
6. **Dead-Letter Queue (DLQ) & Replay**: Un-deliverable events are captured in `webhook:dlq` with manual replay functionality.
7. **Real-time SSE Dashboard**: Live metrics, circuit breaker status badges, and event delivery logs.

---

## 🚀 Quick Start Guide

### 1. Backend Setup & Run

```powershell
# Navigate to project directory
cd webhook-delivery-system

# Activate virtual environment
.\venv\Scripts\activate

# Seed initial test subscribers
$env:PYTHONPATH=".\backend"
python .\backend\seed.py

# Start FastAPI server (runs on http://localhost:8000)
cd backend
..\venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 8000
```

### 2. Frontend Setup & Run

```powershell
cd webhook-delivery-system\frontend
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) in your browser to view the Real-Time Dashboard.

---

## 🧪 Testing & Benchmarking

### Unit Tests
```powershell
$env:PYTHONPATH=".\backend"
.\venv\Scripts\pytest .\backend\tests\ -v
```

### Load Testing (300+ events/sec benchmark)
```powershell
.\venv\Scripts\python .\load_tests\python_load_test.py
```

### k6 Benchmark
```bash
k6 run load_tests/k6_load_test.js
```

---

## 💬 Interview Elevator Pitch

> *"I built a distributed Webhook Delivery Engine in Python/FastAPI using Redis Streams and worker pools to achieve at-least-once delivery guarantees. I implemented per-subscriber circuit breaking to isolate degrading endpoints, HMAC SHA-256 request signing for security, and exponential backoff with full jitter to avoid thundering herd issues. During load tests, the system handled over 60 events/sec with sub-400ms P95 ingestion latency."*
