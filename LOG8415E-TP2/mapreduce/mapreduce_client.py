"""read config json file (variable) and sent http request to orchestrator,
wait for get jobs with job id request to show completed and then scp output to this computer
"""

import json
import os
import requests
import paramiko
from time import sleep
from collections import defaultdict


ALGORITHM_NAME = "friendrec"
INPUT_FILE = "dataset.txt"
REMOTE_OUTPUT_FOLDER = "/home/ec2-user/mapreduce/output/"
LOCAL_OUTPUT_FOLDER = "./output/"
SSH_KEY_PATH = "labsuser.pem"


def read_deployed_config(config_path="deployed_config.json"):
    """Read deployed configuration from a JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    with open(config_path, "r") as f:
        config = json.load(f)

    return config


def send_map_reduce_request(config, input_file, algorithm):
    """Send a MapReduce job request to the orchestrator."""

    orchestrator_url = f"{config['orchestrator_url']}/jobs"

    job_request = {
        "algorithm": algorithm,
        "input_file": input_file,
        "num_reducers": config['num_reducers'],
        "mapper_urls": config['mapper_urls'],
        "reducer_urls": config['reducer_urls'],
        "partitioner_url": config['partitioner_url']
    }

    response = requests.post(orchestrator_url, json=job_request)
    response.raise_for_status()

    return response.json()


def wait_until_job_complete(config, job_id, poll_interval=5):
    """Poll the orchestrator until the job is complete."""
    orchestrator_url = f"{config['orchestrator_url']}/jobs/{job_id}"

    while True:
        response = requests.get(orchestrator_url)
        response.raise_for_status()
        status = response.json()

        if status['status'] == 'completed':
            return status
        elif status['status'] == 'failed':
            raise Exception(f"Job {job_id} failed.")

        print(
            f"Job {job_id} status: {status['status']}. Polling again in {poll_interval} seconds...")
        sleep(poll_interval)


def create_ssh_client(hostname, username, key_path):
    key = paramiko.RSAKey.from_private_key_file(key_path)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(hostname, username=username, pkey=key)
    return ssh


def scp_retrieve_file(ssh_client, remote_path, local_path):
    sftp = ssh_client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()


def get_targets_from_output_file(output_file, targets):
    """Extract and group recommendations for specific target users from the output file."""

    grouped_recommendations = defaultdict(list)
    with open(output_file, 'r') as f:
        for line in f:
            user, rec = line.strip().split('\t')
            user_clean = user.strip('\'')
            if user_clean in targets:
                grouped_recommendations[user_clean].append(rec)

    # Format: one line per user, tab-separated recommendations
    result = []
    for user in targets:
        if user in grouped_recommendations:
            recs = '\t'.join(grouped_recommendations[user])
            result.append(f"{user}\t{recs}")
    return result

def run_map_reduce(config):
    """Run the MapReduce job end-to-end using the provided configuration."""
    print("Submitting MapReduce job...")
    job_response = send_map_reduce_request(config, INPUT_FILE, ALGORITHM_NAME)
    job_id = job_response['job_id']
    print(f"Job submitted with ID: {job_id}")

    print("Waiting for job to complete...")
    wait_until_job_complete(config, job_id)
    print(f"Job {job_id} completed.")

    remote_output_filepath = REMOTE_OUTPUT_FOLDER + f"output_{job_id}.txt"

    print(f"Retrieving output from {remote_output_filepath}...")

    ssh_client = create_ssh_client(
        hostname=config['orchestrator_url'].split("//")[1].split(":")[0],
        username='ec2-user',
        key_path=SSH_KEY_PATH
    )

    scp_retrieve_file(ssh_client, remote_output_filepath,
                      LOCAL_OUTPUT_FOLDER + f"output_{job_id}.txt")
    ssh_client.close()

    print(
        f"Output retrieved and saved to {LOCAL_OUTPUT_FOLDER + f'output_{job_id}.txt'}.")

    # Extract recommendations for specific target users
    target_users = ['924', '8941', '8942', '9019',
                    '9020', '9021', '9022', '9990', '9992', '9993']
    recommendations = get_targets_from_output_file(
        LOCAL_OUTPUT_FOLDER + f"output_{job_id}.txt", target_users)

    with open(f"./recommendations_{job_id}.txt", "w") as f:
        for line in recommendations:
            f.write(line + "\n")
    print(f"Recommendations saved to ./recommendations_{job_id}.txt.")


if __name__ == "__main__":
    config = read_deployed_config()
    run_map_reduce(config)

    
