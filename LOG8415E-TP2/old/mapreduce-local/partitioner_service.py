"""
Partitioner for Multi-Reducer MapReduce
Splits sorted mapper output into n partitions based on key hash
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import hashlib
import uvicorn

app = FastAPI(title="Partitioner Service")

class PartitionRequest(BaseModel):
    sorted_data: List[str]
    num_partitions: int

class PartitionResponse(BaseModel):
    partitions: Dict[int, List[str]]
    partition_counts: Dict[int, int]

def partition_line(line, num_reducers):
    """
    Determine which reducer should handle this line based on key hash
    """
    try:
        # Get first ID from key. We assume the key is in the format "key<TAB>value"
        key = line.split('\t')[0].split(',')[0]
        # It creates an MD5 hash of the key, converts the hexadecimal digest to an integer.
        # MD5 processes a variable-length message into a fixed-length output of 128 bits.
        # https://en.wikipedia.org/wiki/MD5#Algorithm
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        # The hash value is divided by the number of reducers, and the remainder determines which reducer gets the line.
        # Values domain: 0 to num_reducers-1
        # Identical keys will always go to the same reducer
        return hash_value % num_reducers
    except:
        return 0  # Default to first reducer on error

@app.post("/partition", response_model=PartitionResponse)
async def partition_data(request: PartitionRequest):
    """
    Partition sorted mapper output into multiple partition requests for reducers
    """
    try:
        # {0 : [...], 1: [...], ...}
        partitions = {i: [] for i in range(request.num_partitions)}
        
        for record in request.sorted_data:
            if record.strip():
                # Determine which reducer gets this data record
                partition_id = partition_line(record, request.num_partitions)
                partitions[partition_id].append(record)
        
        partition_counts = {i: len(partitions[i]) for i in range(request.num_partitions)}
        
        return PartitionResponse(partitions=partitions, partition_counts=partition_counts)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Partitioner error: {str(e)}")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "partitioner"}

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8005
    uvicorn.run(app, host="0.0.0.0", port=port)
