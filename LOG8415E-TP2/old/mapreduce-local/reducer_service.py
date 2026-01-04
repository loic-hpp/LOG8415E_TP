"""
Generic Reducer Framework
Loads and executes user-defined reduce function from algorithm module
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import importlib.util
from typing import List
import uvicorn

app = FastAPI(title="Reducer Service")

class ReduceRequest(BaseModel):
    algorithm_path: str
    sorted_data: List[str]

class ReduceResponse(BaseModel):
    reduced_data: List[str]
    total_records: int

def load_algorithm(algorithm_file):
    """Dynamically load the algorithm module"""
    spec = importlib.util.spec_from_file_location("algorithm", algorithm_file)
    algorithm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(algorithm)
    return algorithm

@app.post("/reduce", response_model=ReduceResponse)
async def reduce_data(request: ReduceRequest):
    """
    Run generic reducer with user-defined reduce function
    Expected algorithm module to have: reduce_function(key, values) -> output
    """
    try:
        algorithm = load_algorithm(request.algorithm_path)
        
        if not hasattr(algorithm, 'reduce_function'):
            raise HTTPException(status_code=400, detail="Algorithm must have reduce_function")
        
        current_key = None
        values = []
        results = []
        
        for line in request.sorted_data:
            if not line.strip():
                continue
            
            parts = line.split('\t', 1)
            if len(parts) != 2:
                continue
            
            key, value = parts[0], parts[1]
            
            if current_key and current_key != key:
                result = algorithm.reduce_function(current_key, values)
                if result is not None:
                    results.extend(result)
                values = []
            
            current_key = key
            values.append(value)
        
        if current_key:
            result = algorithm.reduce_function(current_key, values)
            if result is not None:
                results.extend(result)
        
        return ReduceResponse(reduced_data=results, total_records=len(results))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reducer error: {str(e)}")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "reducer"}

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8002
    uvicorn.run(app, host="0.0.0.0", port=port)
