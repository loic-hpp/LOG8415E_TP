"""
Generic MapReduce Orchestrator
Works with any user-defined algorithm module
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
import httpx
import asyncio
import importlib.util
import os
from datetime import datetime
import uvicorn
from pathlib import Path

app = FastAPI(title="MapReduce Orchestrator")

jobs = {}

class JobRequest(BaseModel):
    algorithm: Optional[str] = None
    # input_file contains the input data file path provided by the client
    input_file: Optional[str] = None
    num_reducers: Optional[int] = None
    mapper_urls: Optional[List[str]] = None
    reducer_urls: Optional[List[str]] = None
    partitioner_url: Optional[str] = None

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str

class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: str
    result_preview: Optional[List[str]] = None
    partition_counts: Optional[dict] = None
    error: Optional[str] = None

def load_algorithm(algorithm_file):
    """Load user algorithm metadata. We use this approach because we don't know the algorithm at compile time."""
    # The spec contains info about how to load the module
    spec = importlib.util.spec_from_file_location("algorithm", algorithm_file)
    # Creates a new empty module object based on the spec.
    algorithm = importlib.util.module_from_spec(spec)
    # Runs the code in the file, populating the module object with its functions, classes, and variables
    spec.loader.exec_module(algorithm)
    return algorithm

async def run_mapreduce_job(job_id: str, job_request: JobRequest):
    """
    Executes the MapReduce pipeline for a given job:
    1. Reads user algorithm input data.
    2. Sends data chunks to mapper services and collects mapped results.
    3. Sends all mapped data to partitioner service to divide into reducer partitions.
    4. Dispatches each partition to reducer services in parallel and collects reduced results.
    5. Optionally aggregates reducer outputs using the algorithm's aggregate_function.
    6. Saves final results and updates job status.
    """
    try:
        jobs[job_id]["status"] = "running"

        algorithm_path = str(Path("/home/ec2-user") / "mapreduce" / "algorithms" / f"{job_request.algorithm}.py")
        dataset_path = str(Path("/home/ec2-user") / "mapreduce" / "data" / job_request.input_file)

        # 1. Read MapReduce algorithm data
        if not os.path.exists(dataset_path):
            raise HTTPException(status_code=404, detail=f"Input file not found: {dataset_path}")
        if not os.path.isfile(dataset_path):
            raise HTTPException(status_code=400, detail=f"Input path is not a file: {dataset_path}")
        with open(dataset_path, 'r') as f:
            input_lines = [line.strip() for line in f if line.strip()]
        if not input_lines:
            raise HTTPException(status_code=400, detail="Input file is empty or contains only blank lines")
        
        # 2. Map Phase
        # Validate mapper URLs and split input data into chunks for each mapper
        if not job_request.mapper_urls or len(job_request.mapper_urls) == 0:
            raise HTTPException(status_code=400, detail="No mapper_urls provided in job payload")
        
        num_mappers = len(job_request.mapper_urls)
        # Chunk input as evenly as possible
        chunk_size = max(1, len(input_lines) // num_mappers)
        mapper_chunks = []
        for i, mapper_url in enumerate(job_request.mapper_urls):
            # Calculate start and end indices for each chunk
            start = i * chunk_size
            end = start + chunk_size if i < num_mappers - 1 else len(input_lines)
            # Append tuple of (mapper_url, data_chunk)
            mapper_chunks.append((mapper_url, input_lines[start:end]))

        # Send data chunks to mappers
        async with httpx.AsyncClient(timeout=300.0) as client:
            mapper_tasks = [
                client.post(f"{url}/map", json={
                    "algorithm_path": algorithm_path,
                    "data_lines": chunk
                })
                for url, chunk in mapper_chunks
            ]
            # Wait for the mapper responses and unpack them into separate arguments
            mapper_responses = await asyncio.gather(*mapper_tasks)
        
        # Process the asyncio https responses and aggregate all mapped data into a single list
        all_mapped_data = []
        for response in mapper_responses:
            if response.status_code == 200:
                all_mapped_data.extend(response.json()["mapped_data"])
            else:
                raise HTTPException(status_code=500, detail=f"Mapper service at {response.url} failed with status {response.status_code}: {response.text}")
               
        # 3. Partition
        async with httpx.AsyncClient(timeout=300.0) as client:
            partition_response = await client.post(
            f"{job_request.partitioner_url}/partition",
            json={"all_mapped_data": all_mapped_data, "num_partitions": job_request.num_reducers}
            )

            if partition_response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=f"Partitioner service at {partition_response.url} failed with status {partition_response.status_code}: {partition_response.text}"
                )
        
        partitions = partition_response.json()["partitions"]
        partition_counts = partition_response.json()["partition_counts"]
        
        # 4. Reduce Phase
        async with httpx.AsyncClient(timeout=300.0) as client:
            reducer_tasks = []
            for i in range(job_request.num_reducers):
                # Assign each partition to the corresponding reducer URL
                reducer_url = job_request.reducer_urls[i]
                partition_data = partitions.get(str(i), [])
                reducer_tasks.append(
                    client.post(f"{reducer_url}/reduce", json={
                        "algorithm_path": algorithm_path,
                        "partitioned_data": partition_data
                    })
                )
            reducer_responses = await asyncio.gather(*reducer_tasks)
        
        all_reduced_data = []
        for response in reducer_responses:
            if response.status_code == 200:
                all_reduced_data.extend(response.json()["reduced_data"])
            else:
                raise HTTPException(status_code=500, detail=f"Reducer service at {response.url} failed with status {response.status_code}: {response.text}")

        # 5. Aggregate
        try:
            algorithm = load_algorithm(f"algorithms/{job_request.algorithm}.py")
            if hasattr(algorithm, 'aggregate_function'):
                final_results = algorithm.aggregate_function(all_reduced_data)
            else:
                final_results = all_reduced_data
        except:
            final_results = all_reduced_data
        
        # 6. Save results
        output_dir = "/home/ec2-user/mapreduce/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"output_{job_id}.txt"
        with open(os.path.join(output_dir, output_file), 'w') as f:
            for line in final_results:
                f.write(line + '\n')
        
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["result_preview"] = final_results[:10]
        jobs[job_id]["output_file"] = output_file
        jobs[job_id]["partition_counts"] = partition_counts
        
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)

@app.post("/jobs", response_model=JobResponse)
async def create_job(job_request: JobRequest, background_tasks: BackgroundTasks):
    """Create and start a new MapReduce job.

    The client must provide `input_file` (path to the input text file) as part of the
    job payload. The orchestrator will no longer attempt to read files from local disk.
    """
    # Build a complete request dict by using provided fields
    req = {
        "algorithm": job_request.algorithm,
        "input_file": job_request.input_file,
        "num_reducers": job_request.num_reducers,
        "mapper_urls": job_request.mapper_urls,
        "reducer_urls": job_request.reducer_urls,
        "partitioner_url": job_request.partitioner_url
    }

    # Validate that input content was provided
    if not req["input_file"]:
        raise HTTPException(status_code=400, detail="No input_file provided in job payload")

    # Recreate a JobRequest object with values
    merged_request = JobRequest(**req)

    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    jobs[job_id] = {"job_id": job_id, "status": "pending", "progress": "Job created"}

    background_tasks.add_task(run_mapreduce_job, job_id, merged_request)

    return JobResponse(job_id=job_id, status="pending", message="Job queued")

@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get status and progress of a MapReduce job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatus(**jobs[job_id])

@app.get("/jobs/{job_id}/output")
async def get_job_output(job_id: str):
    """Fetch the content of a job's output file"""
    job = jobs.get(job_id)
    if not job or "output_file" not in job:
        raise HTTPException(status_code=404, detail="Output file not found for this job")
    output_path = os.path.join("/home/ec2-user/mapreduce/output", job["output_file"])
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file does not exist")
    with open(output_path, "r") as f:
        content = f.read()
    return {"job_id": job_id, "file_name": job["output_file"], "output": content}

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "orchestrator"}

@app.get("/logs")
async def get_service_log():
    """Fetch the content of the orchestrator log file."""
    service_file = os.path.basename(__file__)
    log_path = f"/tmp/{service_file}.log"
    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail="Log file does not exist")
    with open(log_path, "r") as f:
        content = f.read()
    return {"log_file": log_path, "content": content}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
