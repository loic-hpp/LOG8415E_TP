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

app = FastAPI(title="MapReduce Orchestrator")

jobs = {}

class JobRequest(BaseModel):
    algorithm: str
    input_file: str
    num_reducers: int = 3
    mapper_urls: List[str] = ["http://localhost:8001"]
    reducer_urls: List[str] = ["http://localhost:8002", "http://localhost:8003", "http://localhost:8004"]
    partitioner_url: str = "http://localhost:8005"

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
    3. Sorts all mapped output by key.
    4. Sends sorted data to partitioner service to divide into reducer partitions.
    5. Dispatches each partition to reducer services in parallel and collects reduced results.
    6. Optionally aggregates reducer outputs using the algorithm's aggregate_function.
    7. Saves final results and updates job status.
    """
    try:
        jobs[job_id]["status"] = "running"
        
        # 1. Read MapReduce algorithm data
        with open(job_request.input_file, 'r') as f:
            input_lines = [line.strip() for line in f if line.strip()]
        
        # 2. Map Phase
        # Split input data into chunks for each mapper
        chunk_size = len(input_lines) // len(job_request.mapper_urls)
        mapper_chunks = []
        for i, mapper_url in enumerate(job_request.mapper_urls):
            start = i * chunk_size
            end = start + chunk_size if i < len(job_request.mapper_urls) - 1 else len(input_lines)
            # (http://mapper_url, [data_lines])
            mapper_chunks.append((mapper_url, input_lines[start:end]))
        
        async with httpx.AsyncClient(timeout=300.0) as client:
            mapper_tasks = [
                client.post(f"{url}/map", json={
                    "algorithm_path": f"algorithms/{job_request.algorithm}.py",
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
        
        # 3. Sort
        # Use the python built-in sort function to sort by the data key (part before the tab)
        sorted_data = sorted(all_mapped_data, key=lambda x: x.split('\t')[0])
        
        # 4. Partition
        async with httpx.AsyncClient(timeout=300.0) as client:
            partition_response = await client.post(
                f"{job_request.partitioner_url}/partition",
                json={"sorted_data": sorted_data, "num_partitions": job_request.num_reducers}
            )
        
        partitions = partition_response.json()["partitions"]
        partition_counts = partition_response.json()["partition_counts"]
        
        # 5. Reduce Phase
        async with httpx.AsyncClient(timeout=300.0) as client:
            reducer_tasks = []
            for i in range(job_request.num_reducers):
                reducer_url = job_request.reducer_urls[i % len(job_request.reducer_urls)]
                partition_data = partitions.get(str(i), [])
                reducer_tasks.append(
                    client.post(f"{reducer_url}/reduce", json={
                        "algorithm_path": f"algorithms/{job_request.algorithm}.py",
                        "sorted_data": partition_data
                    })
                )
            reducer_responses = await asyncio.gather(*reducer_tasks)
        
        all_reduced_data = []
        for response in reducer_responses:
            if response.status_code == 200:
                all_reduced_data.extend(response.json()["reduced_data"])
        
        # 6. Aggregate
        try:
            algorithm = load_algorithm(f"algorithms/{job_request.algorithm}.py")
            if hasattr(algorithm, 'aggregate_function'):
                final_results = algorithm.aggregate_function(all_reduced_data)
            else:
                final_results = all_reduced_data
        except:
            final_results = all_reduced_data
        
        # 7. Save results
        output_file = f"output_{job_id}.txt"
        with open(f"./output/{output_file}", 'w') as f:
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
    """Create and start a new MapReduce job"""
    if not os.path.exists(job_request.input_file):
        raise HTTPException(status_code=400, detail="Input file not found")
    
    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    jobs[job_id] = {"job_id": job_id, "status": "pending", "progress": "Job created"}
    
    background_tasks.add_task(run_mapreduce_job, job_id, job_request)
    
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
    output_path = os.path.join("output", job["output_file"])
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file does not exist")
    with open(output_path, "r") as f:
        content = f.read()
    return {"job_id": job_id, "file_name": job["output_file"], "output": content}

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "orchestrator"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
