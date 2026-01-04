import os
from fastapi import FastAPI
import uvicorn
import logging

instance_id = os.getenv("INSTANCE_ID", "unknown")

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

@app.get("/cluster2")
async def cluster1():
    message = "Instance " + instance_id + " has received the request on cluster 2"
    logger.info(message)
    return {"message": message}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
