import asyncio
import time
import httpx

API_URL = "http://localhost:8000/api/v1/events"
TOTAL_REQUESTS = 300
CONCURRENCY = 15

async def send_event(client: httpx.AsyncClient, i: int) -> float:
    start = time.time()
    payload = {
        "event_type": "payment.succeeded" if i % 2 == 0 else "order.created",
        "payload": {"amount": 1000 + i, "customer_id": f"cust_{i}"},
        "idempotency_key": f"load-test-idem-{i}"
    }
    try:
        resp = await client.post(API_URL, json=payload, timeout=10.0)
        duration_ms = (time.time() - start) * 1000
        return duration_ms if resp.status_code == 202 else -1.0
    except Exception as e:
        return -1.0

async def run_benchmark():
    print(f"Starting Load Benchmark against {API_URL}...")
    print(f"Sending {TOTAL_REQUESTS} requests with concurrency level {CONCURRENCY}...")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    
    async def worker(client, i):
        async with semaphore:
            return await send_event(client, i)

    start_total = time.time()
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=CONCURRENCY * 2)) as client:
        tasks = [worker(client, i) for i in range(TOTAL_REQUESTS)]
        latencies = await asyncio.gather(*tasks)

    total_time = time.time() - start_total
    valid_latencies = [l for l in latencies if l > 0]
    failures = len(latencies) - len(valid_latencies)
    
    valid_latencies.sort()
    avg_latency = sum(valid_latencies) / len(valid_latencies) if valid_latencies else 0
    p95_index = int(len(valid_latencies) * 0.95)
    p95_latency = valid_latencies[p95_index] if valid_latencies else 0
    p99_index = int(len(valid_latencies) * 0.99)
    p99_latency = valid_latencies[p99_index] if valid_latencies else 0
    rps = len(valid_latencies) / total_time

    print("\n================ LOAD TEST RESULTS ================")
    print(f"Total Ingested Events : {len(valid_latencies)} / {TOTAL_REQUESTS}")
    print(f"Failed Requests       : {failures}")
    print(f"Total Execution Time  : {total_time:.2f} seconds")
    print(f"Throughput (RPS)      : {rps:.1f} events/sec")
    print(f"Average Latency       : {avg_latency:.2f} ms")
    print(f"P95 Latency           : {p95_latency:.2f} ms")
    print(f"P99 Latency           : {p99_latency:.2f} ms")
    print("====================================================\n")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
