"""Client script to submit and monitor MapReduce jobs.
"""

import os
import time
import json
import requests
from pathlib import Path
from infrastructure_provisioning import deploy_and_get_config

BASE_DIR = os.path.dirname(__file__)
cfg_path = Path(__file__).parent / "configs" / "mapreduce_config.json"

# 1. Deploy infrastructure and get runtime config
deployed = None
try:
    deployed = deploy_and_get_config()
except Exception as e:
    raise RuntimeError(f"Failed to import deploy_and_get_config from TP2.py: {e}")

with open(cfg_path, "r") as f:
    base_cfg = json.load(f)

payload = {
    "algorithm": base_cfg.get("algorithm"),
    # Read input file locally and send content as lines
    "input_lines": [],
    "num_reducers": base_cfg.get("num_reducers")
}

# 2. Update the MapReduce payload with runtime endpoints
if deployed:
    # Add runtime endpoints returned by deployer
    payload["mapper_urls"] = deployed.get("mapper_urls")
    payload["reducer_urls"] = deployed.get("reducer_urls")
    payload["partitioner_url"] = deployed.get("partitioner_url")
    # Optionally orchestrator_url can be used for health checks
    orchestrator_url = deployed.get("orchestrator_url")

    if not payload["mapper_urls"] or not payload["reducer_urls"] or not payload["partitioner_url"] or not orchestrator_url:
        raise RuntimeError("One or more service URLs not found in deployed config.")
else:
    raise RuntimeError("Deployed config not available. Please deploy the infrastructure first.")

# 3. Load data input file content locally and attach to payload
input_path = os.path.join(BASE_DIR, base_cfg.get("input_file"))
if not os.path.exists(input_path):
    raise RuntimeError(f"Input file not found: {input_path}")
with open(input_path, "r") as f:
    payload["input_lines"] = [line.strip() for line in f if line.strip()]

# 4. Submit MapReduce job
response = requests.post(f"{orchestrator_url}/jobs", json=payload)
job_id = response.json().get("job_id")
print(f"Job submitted: {job_id}")

# 5. Poll for completion of the MapReduce job
while True:
    status = requests.get(f"{orchestrator_url}/jobs/{job_id}").json()
    print(f"Status: {status['status']} - {status['progress']}")

    if status["status"] in ["completed", "failed"]:
        break
    time.sleep(2)

# 6. Save MapReduce job results
if status["status"] == "completed":
    print("Reducer partition counts:", status.get("partition_counts", {}))

    print("\nResults preview:")
    for line in status["result_preview"]:
        print(line)

    # Fetch and save full output file
    output_response = requests.get(f"{orchestrator_url}/jobs/{job_id}/output")
    if output_response.status_code == 200:
        output_data = output_response.json()
        output_filename = output_data.get("file_name", "output.txt")
        with open(f'./output/{output_filename}', "w") as f:
                f.write(output_data["output"])
        print(f"\nFull output file saved to ./{output_filename}")
    else:
        print(f"Failed to fetch output file: {output_response.text}")

elif status["status"] == "failed":
    print(f"Job failed. Reason: {status.get('error', 'Unknown error')}")
    exit(1)
