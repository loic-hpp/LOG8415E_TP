"""
Generic Mapper Framework
Loads and executes user-defined map function from algorithm module
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import importlib.util
from typing import List
import uvicorn

app = FastAPI(title="Mapper Service")

class MapRequest(BaseModel):
    algorithm_path: str
    data_lines: List[str]

class MapResponse(BaseModel):
    mapped_data: List[str]
    total_records: int

def load_algorithm(algorithm_file):
    """Dynamically load the algorithm module"""
    spec = importlib.util.spec_from_file_location("algorithm", algorithm_file)
    algorithm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(algorithm)
    return algorithm

@app.post("/map", response_model=MapResponse)
async def map_data(request: MapRequest):
    """
    Run generic mapper with user-defined map function
    Expected algorithm module to have: map_function(line, emit)
    """
    try:
        algorithm = load_algorithm(request.algorithm_path)
        
        if not hasattr(algorithm, 'map_function'):
            raise HTTPException(status_code=400, detail="Algorithm must have map_function")
        
        emissions = []
        
        def emit(key, value):
            emissions.append(f"{key}\t{value}")
        
        for line in request.data_lines:
            if line.strip():
                algorithm.map_function(line.strip(), emit)
        
        return MapResponse(mapped_data=emissions, total_records=len(emissions))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mapper error: {str(e)}")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "mapper"}

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    uvicorn.run(app, host="0.0.0.0", port=port)
