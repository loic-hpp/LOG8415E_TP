import os
from fastapi import FastAPI
import uvicorn
import logging

instance_id = os.getenv("INSTANCE_ID", "unknown")
cluster_id = os.getenv("CLUSTER_NAME", "unknown")
cluster_name = "cluster 1" if os.getenv("CLUSTER_NAME", "unknown") == "cluster1" else "cluster 2"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI()

@app.get("/")
async def root():
    message = "Instance " + instance_id + "  has received the request"
    logger.info(message)
    return {"message": message}

@app.get(f"/{cluster_id}")
async def cluster1():
    message = f"Instance {instance_id} has received the request on {cluster_name}"
    logger.info(message)
    return {"message": message}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
