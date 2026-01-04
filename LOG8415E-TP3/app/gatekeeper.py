import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AUTH_KEY = "tp3-log8415e"
TRUSTED_HOST_IP = os.getenv("PROXY_IP", "unknown")
APP_PORT = 8000

app = FastAPI()


def handle_hit(payload: dict, endpoint: str):
    """Shared validation + forwarding logic for all endpoints."""

    # Validate auth key
    if payload.get("auth_key") is None:
        return JSONResponse(
            status_code=401, content={"error": "Unauthorized missing auth_key"}
        )

    if payload.get("auth_key") != AUTH_KEY:
        return JSONResponse(
            status_code=401, content={"error": "Unauthorized wrong auth_key"}
        )

    query = payload.get("query")

    # Validate query field
    if not query:
        return JSONResponse(
            status_code=400, content={"error": "Bad Request: 'query' field is required"}
        )

    # Malicious query checks
    if query.startswith(("DROP", "DELETE", "drop", "delete")) or "--" in query:
        return JSONResponse(
            status_code=403, content={"error": "Forbidden: Malicious query detected"}
        )

    if not query.startswith(
        ("SELECT", "select", "UPDATE", "update", "INSERT", "insert")
    ):
        return JSONResponse(
            status_code=400,
            content={
                "error": "Bad Request: Only SELECT, UPDATE, INSERT queries are allowed"
            },
        )

    # Forward request to trusted host
    url = f"http://{TRUSTED_HOST_IP}:{APP_PORT}/{endpoint}"
    response = requests.post(url, json={"query": query})
    if response.status_code != 200:
        return JSONResponse(
            status_code=response.status_code,
            content={"error": "Error from trusted host"},
        )

    return {"data": response.json()}


@app.post("/directHit")
async def direct_hit(request: Request):
    try:
        payload = await request.json()
        return handle_hit(payload, "directHit")
    except Exception as e:
        logger.error("Error in direct_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@app.post("/randomHit")
async def random_hit(request: Request):
    try:
        payload = await request.json()
        return handle_hit(payload, "randomHit")
    except Exception as e:
        logger.error("Error in random_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@app.post("/customHit")
async def custom_hit(request: Request):
    try:
        payload = await request.json()
        return handle_hit(payload, "customHit")
    except Exception as e:
        logger.error("Error in custom_hit: %s", e)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
