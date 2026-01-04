import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import subprocess
import requests
import uvicorn
import logging
import pymysql
import random
import time
import threading


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
best_worker_ip = None
INTERVAL = 0.1  # 100 ms
TIMEOUT = 2  # seconds
APP_PORT = 8000
MASTER_IP = os.getenv("MASTER_IP", "unknown")
WORKER_IP_STRING = os.getenv("WORKER_IPS", "unknown")
WORKER_IPS = WORKER_IP_STRING.split(",") if WORKER_IP_STRING else []
logger.info(f"WORKER_IP_STRING : {WORKER_IP_STRING}")
logger.info(f"Worker IPs: {WORKER_IPS}")
MYSQL_USER = "log8415"
MYSQL_PASSWORD = "log8415Pwd@"

app = FastAPI()
@app.on_event("startup")
def start_best_worker_thread():
    logger.info("Starting best worker thread...")
    threading.Thread(target=set_best_worker, daemon=True).start()

def connect_to_db(host: str):
    return pymysql.connect(
        host=host,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database="sakila",
        ssl_disabled=True,
    )


def execute_query_on_worker(worker_ip: str, query: str):
    mysql_conn = connect_to_db(worker_ip)
    cursor = mysql_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    if query.startswith(("UPDATE", "update", "INSERT", "insert")):
        mysql_conn.commit()
        results = "Update query executed successfully"
    cursor.close()
    mysql_conn.close()
    return results


def set_best_worker():
    global best_worker_ip
    while True:
        best = None
        best_time = float("inf")

        for worker_ip in WORKER_IPS:
            try:
                start = time.perf_counter()
                result = subprocess.run(
                    ["ping", "-c", "1", "-W", "1", worker_ip],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                # check if ping was successful
                if result.returncode != 0:
                    logger.info(f"Worker {worker_ip} is unreachable.")
                    continue
                elapsed = time.perf_counter() - start
                if elapsed < best_time:
                    best_time = elapsed
                    best = worker_ip
            except Exception:
                pass  # ignore unhealthy instances

        best_worker_ip = best
        time.sleep(INTERVAL)


@app.post("/directHit")
async def direct_hit(request: Request):
    try:
        payload = await request.json()
        query = payload.get("query")
        results = execute_query_on_worker(MASTER_IP, query)
        return {"worker_ip": MASTER_IP, "query": query, "results": results}
    except Exception as e:
        logger.error("Error in direct_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})
    except Exception as e:
        logger.error("Error in direct_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@app.post("/randomHit")
async def random_hit(request: Request):
    try:
        payload = await request.json()
        query = payload.get("query")
        if query.startswith(("UPDATE", "update", "INSERT", "insert")):
            worker_ip = MASTER_IP
        else:
            worker_ip = random.choice(WORKER_IPS)
        results = execute_query_on_worker(worker_ip, query)
        return {"worker_ip": worker_ip, "query": query, "results": results}
    except Exception as e:
        logger.error("Error in random_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@app.post("/customHit")
async def custom_hit(request: Request):
    try:
        payload = await request.json()
        query = payload.get("query")
        if query.startswith(("UPDATE", "update", "INSERT", "insert")):
            worker_ip = MASTER_IP
        else:
            worker_ip = best_worker_ip
        results = execute_query_on_worker(worker_ip, query)
        return {"worker_ip": worker_ip, "query": query, "results": results}
    except Exception as e:
        logger.error("Error in custom_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
