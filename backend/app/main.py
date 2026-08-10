import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_db
from app.queue.redis_queue import queue_client
from app.worker.delivery_worker import delivery_engine
from app.api import subscribers, events, sse, mock_endpoints

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    init_db()
    await queue_client.connect()
    
    # Start worker background task
    worker_task = asyncio.create_task(delivery_engine.start_worker_loop())
    
    yield
    
    # Shutdown actions
    delivery_engine.stop()
    worker_task.cancel()
    await queue_client.close()

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# CORS Middleware setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(subscribers.router, prefix=settings.API_V1_STR)
app.include_router(events.router, prefix=settings.API_V1_STR)
app.include_router(sse.router, prefix=settings.API_V1_STR)
app.include_router(mock_endpoints.router, prefix=settings.API_V1_STR)

@app.get("/")
def root():
    return {
        "system": settings.PROJECT_NAME,
        "status": "operational",
        "docs": "/docs",
        "sse_stream": f"{settings.API_V1_STR}/events/stream"
    }
