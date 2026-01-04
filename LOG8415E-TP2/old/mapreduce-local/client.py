"""Client script to submit and monitor MapReduce jobs."""

import requests
import time

CONFIG = {
    "algorithm": "wordcount",
    "input_file": "sample_text.txt",
    "num_reducers": 3
}

# Submit job
response = requests.post("http://localhost:8000/jobs", json=CONFIG)
job_id = response.json()["job_id"]
print(f"Job submitted: {job_id}")

# Poll for completion
while True:
    """Check job status : {running, completed, failed}"""
    status = requests.get(f"http://localhost:8000/jobs/{job_id}").json()
    print(f"Status: {status['status']} - {status['progress']}")

    if status["status"] in ["completed", "failed"]:
        break
    time.sleep(2)

# View results
if status["status"] == "completed":
    print("Reducer partition counts:", status.get("partition_counts", {}))

    print("\nResults preview:")
    for line in status["result_preview"]:
        print(line)

# Fetch and save full output file
output_response = requests.get(f"http://localhost:8000/jobs/{job_id}/output")
if output_response.status_code == 200:
    output_data = output_response.json()
    output_filename = output_data.get("file_name", "output.txt")
    with open(output_filename, "w") as f:
        f.write(output_data["output"])
    print(f"\nFull output file saved to ./{output_filename}")
else:
    print(f"Failed to fetch output file: {output_response.text}")
