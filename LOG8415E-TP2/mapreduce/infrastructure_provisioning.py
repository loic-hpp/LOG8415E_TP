"""Deployment helper for the TP2 MapReduce project.

Provision EC2 instances for the MapReduce services, upload the
``mapreduce/`` package, start each FastAPI service (orchestrator, mappers,
partitioner, reducers), wait for their ``/health`` endpoints, and return
to the client a small "deployed configuration" JSON describing the service endpoints.
"""
import os
import time
import json
import stat
import boto3
import paramiko
import requests
from pathlib import Path

# Load base config from mapreduce_config.json
cfg_path = Path(__file__).parent / "configs" / "mapreduce_config.json"
if not cfg_path.exists():
    raise SystemExit(f"Missing configuration file: {cfg_path}")
with open(cfg_path, "r") as f:
    CONFIG = json.load(f)

# AWS configuration - read from file mapreduce/aws_config.json
aws_cfg_path = Path(__file__).parent / "configs" / "aws_config.json"
if not aws_cfg_path.exists():
    raise SystemExit(f"Missing AWS configuration file: {aws_cfg_path}")
with open(aws_cfg_path, "r") as f:
    AWS_CONFIG = json.load(f)

# Ports mapping (service -> port)
PORTS = {
    "orchestrator": 8000,
    "mapper": 8001,
    "partitioner": 8005,
    "reducer": 8002,
}

# Directory containing the mapreduce package on the EC2 instances
LOCAL_MAPREDUCE_DIR = Path(__file__).parent.resolve()
REMOTE_APP_DIR = "/home/ec2-user/mapreduce"

def make_userdata(remote_dir: str = REMOTE_APP_DIR) -> str:
    """Return a shell userdata script that prepares the EC2 instance.

    The script installs Python/git, creates the remote application directory
    and sets ownership to ec2-user. The default remote_dir is
    ``/home/ec2-user/mapreduce`` but it can be overridden for testing.
    """
    return f"""#!/bin/bash
yum update -y || true
yum install -y python3 git || true
mkdir -p {remote_dir}
chown -R ec2-user:ec2-user {remote_dir}
"""

ec2 = boto3.resource("ec2", region_name=AWS_CONFIG["region"])
ec2_client = boto3.client("ec2", region_name=AWS_CONFIG["region"])

def ensure_security_group():
    """
    Ensures that an EC2 security group exists with SSH and required service ports open.

    This function checks if a security group with the name specified in AWS_CONFIG["security_group_name"]
    already exists. If it does, it returns its GroupId. Otherwise, it creates a new security group,
    authorizes ingress for SSH (port 22) and the ports specified in the PORTS dictionary (orchestrator,
    mapper, partitioner, reducer), and returns the new GroupId.

    Returns:
        str: The GroupId of the ensured or newly created security group.
    """
    # Ensures a security group is present and properly configured for MapReduce TP2.
    """Create a security group that allows SSH and the service ports."""
    sg_name = AWS_CONFIG["security_group_name"]
    # try to find existing sg
    sgs = ec2_client.describe_security_groups(Filters=[{"Name":"group-name","Values":[sg_name]}])["SecurityGroups"]
    if sgs:
        return sgs[0]["GroupId"]
    resp = ec2_client.create_security_group(
        GroupName=sg_name,
        Description="MapReduce TP2 security group"
    )
    sg_id = resp["GroupId"]
    # Authorize SSH for all MapReduce EC2 instances
    ip_permissions = [
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
         "IpRanges":[{"CidrIp":"0.0.0.0/0"}]}  # Should be tighten for production
    ]
    # Add ip permissions for each EC2 service based on the PORTS dictionary
    ports = [PORTS["orchestrator"], PORTS["mapper"], PORTS["partitioner"], PORTS["reducer"]]
    for p in set(ports):
        ip_permissions.append({"IpProtocol":"tcp", "FromPort":p, "ToPort":p, "IpRanges":[{"CidrIp":"0.0.0.0/0"}]})
    ec2_client.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=ip_permissions)
    return sg_id

def launch_instance(name, subnet_id=None, userdata_script=None, security_group_id=None):
    """Launch an EC2 instance and return (instance_id, public_ip) after it is running."""
    params = {
        "ImageId": AWS_CONFIG["ami_id"],
        "InstanceType": AWS_CONFIG["instance_type"],
        "KeyName": AWS_CONFIG["key_name"],
        "MinCount": 1,
        "MaxCount": 1,
        "TagSpecifications": [{"ResourceType":"instance","Tags":[{"Key":"Name","Value":name}]}],
    }
    # If the user `subnet_id` is not provided and the AWS account has a default VPC,
    # the EC2 instance will be placed in its default subnet.
    if subnet_id:
        params["SubnetId"] = subnet_id
    if security_group_id:
        # use SecurityGroupIds so the instance is launched with the created SG
        params["SecurityGroupIds"] = [security_group_id]
    if userdata_script:
        params["UserData"] = userdata_script
    inst = ec2.create_instances(**params)[0]
    inst.wait_until_running()
    inst.reload()
    return inst.id, inst.public_ip_address

def ssh_connect(ip, key_path, username="ec2-user", timeout=120):
    """Return an active Paramiko SSHClient connected to ip."""
    key = paramiko.RSAKey.from_private_key_file(key_path)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # retry until SSH is ready
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client.connect(ip, username=username, pkey=key, timeout=10)
            return client
        except Exception:
            time.sleep(3)
    raise RuntimeError(f"SSH connect to {ip} failed")

def sftp_upload_dir(sftp, local_dir: Path, remote_dir: str):
    """Recursively upload local_dir to remote_dir via sftp client (paramiko)."""
    if not local_dir.exists():
        raise FileNotFoundError(f"Local directory to upload not found: {local_dir}")
    try:
        sftp.stat(remote_dir)
    except IOError:
        sftp.mkdir(remote_dir)
    for item in local_dir.iterdir():
        remote_path = remote_dir + "/" + item.name
        if item.is_dir():
            try:
                sftp.stat(remote_path)
            except IOError:
                sftp.mkdir(remote_path)
            sftp_upload_dir(sftp, item, remote_path)
        else:
            sftp.put(str(item), remote_path)
            # ensure scripts are executable
            if item.suffix == ".py":
                sftp.chmod(remote_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

def start_service_over_ssh(ssh_client, service_file):
    """Install deps and start uvicorn server for the given service file on the remote host."""
    commands = [
        "sudo yum update -y || true",
        "sudo yum install -y python3 git || true",
        "python3 -m pip install --upgrade pip",
        "python3 -m pip install fastapi pydantic uvicorn httpx requests asyncio || true",
        # run service in background
        f"nohup python3 {service_file} &>/tmp/{os.path.basename(service_file)}.log &"
    ]
    verbose = CONFIG.get("verbose", False)
    for cmd in commands:
        if verbose:
            print(f"Running command: {cmd}")
        stdin, stdout, stderr = ssh_client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode()
        err = stderr.read().decode()
        if verbose:
            print(f"Exit status: {exit_status}")
            if out:
                print(f"STDOUT:\n{out}")
            if err:
                print(f"STDERR:\n{err}")
        # continue even if some commands fail (user can inspect logs)

# After starting services, wait for their /health endpoints to respond
def wait_for_health(url: str, timeout: int = 300, interval: int = 3):
    """Poll the given URL until it returns a successful response or timeout.

    Raises RuntimeError if the URL doesn't become healthy within timeout.
    """
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return True
        except Exception as e:
            last_exc = e
        time.sleep(interval)
    raise RuntimeError(f"Health check failed for {url} after {timeout}s. Last error: {last_exc}")
    
def deploy_and_get_config():
    """Launch instances, deploy services and return the deployed configuration dict.

    Returns a dict with keys: orchestrator_url, mapper_urls, reducer_urls, partitioner_url,
    algorithm, input_file, num_reducers
    """
    sg_id = ensure_security_group()
    subnet = AWS_CONFIG.get("subnet_id")

    # userdata installs python/pip and creates ec2-user home directory
    userdata = make_userdata()

    # Launch orchestrator
    print("Launching orchestrator...")
    oid, oip = launch_instance("mapreduce-orchestrator", subnet_id=subnet, userdata_script=userdata, security_group_id=sg_id)
    print("orchestrator:", oip)

    # Launch mappers
    print("Launching mappers...")
    num_mappers = CONFIG.get("num_mappers", 1)
    mapper_ips = []
    for i in range(num_mappers):
        name = f"mapreduce-mapper-{i}"
        mid, mip = launch_instance(name, subnet_id=subnet, userdata_script=userdata, security_group_id=sg_id)
        print(f"mapper {i}:", mip)
        mapper_ips.append(mip)

    # Launch reducers
    print("Launching reducers...")
    reducer_ips = []
    for i in range(CONFIG.get("num_reducers", 3)):
        name = f"mapreduce-reducer-{i}"
        rid, rip = launch_instance(name, subnet_id=subnet, userdata_script=userdata, security_group_id=sg_id)
        print(f"reducer {i}:", rip)
        reducer_ips.append(rip)

    print("Launching partitioner...")
    pid, pip = launch_instance("mapreduce-partitioner", subnet_id=subnet, userdata_script=userdata, security_group_id=sg_id)
    print("partitioner:", pip)

    # Wait a short while for SSH to be available
    print("Waiting 30 seconds for SSH to become available on all instances...")
    time.sleep(30)

    # Create MapReduce service deployment package specification for deployment
    instances = [
        ("orchestrator", oip, f"{REMOTE_APP_DIR}/orchestrator_service.py"),
        ("partitioner", pip, f"{REMOTE_APP_DIR}/partitioner_service.py"),
    ]
    # add mapper instances
    for idx, mip in enumerate(mapper_ips):
        instances.append((f"mapper-{idx}", mip, f"{REMOTE_APP_DIR}/mapper_service.py"))
    for idx, rip in enumerate(reducer_ips):
        instances.append((f"reducer-{idx + 1}", rip, f"{REMOTE_APP_DIR}/reducer_service.py"))

    # Connect via SSH and deploy services
    print("Deploying services runtime code to all instances...")
    key_path = AWS_CONFIG["key_path"]
    for name, ip, remote_service_path in instances:
        print(f"Deploying {name} -> {ip}")
        ssh = ssh_connect(ip, key_path)
        sftp = ssh.open_sftp()
        # ensure remote app dir exists
        try:
            sftp.stat(REMOTE_APP_DIR)
        except IOError:
            sftp.mkdir(REMOTE_APP_DIR)
        # upload entire local mapreduce dir
        sftp_upload_dir(sftp, LOCAL_MAPREDUCE_DIR, REMOTE_APP_DIR)
        sftp.close()
        # start service
        start_service_over_ssh(ssh, remote_service_path)
        ssh.close()

    # Create deployment configuration for the client
    deployed = {
        "orchestrator_url": f"http://{oip}:{PORTS['orchestrator']}",
        "mapper_urls": [f"http://{ip}:{PORTS['mapper']}" for ip in mapper_ips],
        "reducer_urls": [f"http://{ip}:{PORTS['reducer']}" for ip in reducer_ips],
        "partitioner_url": f"http://{pip}:{PORTS['partitioner']}",
        "algorithm": CONFIG.get("algorithm"),
        "input_file": CONFIG.get("input_file"),
        "num_reducers": CONFIG.get("num_reducers", 3),
        "num_mappers": CONFIG.get("num_mappers", 1)
    }

    # Build list of health endpoints
    health_urls = []
    health_urls.append(f"{deployed['orchestrator_url']}/health")
    for m in deployed.get('mapper_urls', []):
        health_urls.append(f"{m}/health")
    for r in deployed.get('reducer_urls', []):
        health_urls.append(f"{r}/health")
    health_urls.append(f"{deployed['partitioner_url']}/health")

    # Wait for all services to become healthy
    print("Waiting for services to become healthy...")
    for url in health_urls:
        print(f"  checking {url}")
        wait_for_health(url, timeout=300, interval=3)
        print(f"  {url} is healthy")

    print("All services healthy. Returning deployed configuration.")
    return deployed


def main():
    deployed = deploy_and_get_config()

    with open("deployed_config.json", "w") as f:
        json.dump(deployed, f, indent=4)
    print("Deployed configuration written to deployed_config.json")


if __name__ == "__main__":
    main()
