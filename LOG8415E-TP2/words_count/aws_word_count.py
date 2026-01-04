import boto3
import logging
import os
from pathlib import Path
from paramiko import SSHClient
import paramiko

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
INSTANCE_NAME = "wordcount-single-node"
DEFAULT_SSH_USER = os.getenv("EC2_SSH_USER", "ubuntu")
PRIVATE_KEY_PATH = os.getenv("EC2_KEY_PATH", str(Path(__file__).parent / "labsuser.pem"))
KEY_PAIR_NAME = "labsuser"

# AWS clients
ec2 = boto3.resource("ec2")
ec2c = boto3.client("ec2")


def get_ami_id() -> str:
    """Ubuntu 22.04 LTS via SSM public parameter (works across regions)."""
    ssm = boto3.client("ssm")
    for name in (
        "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
        "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    ):
        try:
            return ssm.get_parameter(Name=name)["Parameter"]["Value"]
        except Exception:
            continue
    raise RuntimeError("Could not resolve Ubuntu 22.04 AMI via SSM")

def get_default_vpc_id() -> str:
    resp = ec2c.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    vpcs = resp.get("Vpcs", [])
    if not vpcs:
        raise RuntimeError("No default VPC in this region")
    return vpcs[0]["VpcId"]

def get_one_default_subnet(vpc_id: str) -> str:
    resp = ec2c.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    subs = sorted(resp.get("Subnets", []), key=lambda s: s["AvailabilityZone"])
    if not subs:
        raise RuntimeError("No subnets in default VPC")
    return subs[0]["SubnetId"]

def get_security_group_id(vpc_id: str, name: str = "default") -> str:
    resp = ec2c.describe_security_groups(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "group-name", "Values": [name]}]
    )
    sgs = resp.get("SecurityGroups", [])
    if not sgs:
        raise RuntimeError(f"Cannot find security group {name} in VPC {vpc_id}")
    return sgs[0]["GroupId"]

def allow_my_ip_all(sg_id: str) -> None:
    """Open the SG for this public IP to simplify SSH access (minimal setup)."""
    try:
        my_ip = os.popen("curl -s https://checkip.amazonaws.com").read().strip()
        ec2c.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "-1",
                "FromPort": -1,
                "ToPort": -1,
                "IpRanges": [{"CidrIp": f"{my_ip}/32", "Description": "lab access"}],
            }],
        )
        logger.info(f"Opened SG {sg_id} to {my_ip}/32")
    except Exception as e:
        if "InvalidPermission.Duplicate" in str(e):
            logger.info("Ingress already present; continuing")
        else:
            logger.warning(f"Could not add ingress to SG {sg_id}: {e}")

def wait_running_ok(instance) -> None:
    instance.wait_until_running()
    instance.reload()
    waiter = ec2c.get_waiter("instance_status_ok")
    waiter.wait(InstanceIds=[instance.id])

def create_instance(ami_id: str, subnet_id: str, sg_id: str):
    inst = ec2.create_instances(
        ImageId=ami_id,
        InstanceType="t2.large",
        KeyName=KEY_PAIR_NAME,
        MinCount=1,
        MaxCount=1,
        SubnetId=subnet_id,
        SecurityGroupIds=[sg_id],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": INSTANCE_NAME}],
        }],
    )[0]
    return inst

def _load_private_key(path: str):
    for Key in (paramiko.RSAKey, getattr(paramiko, "ECDSAKey", None), getattr(paramiko, "Ed25519Key", None)):
        if Key is None:
            continue
        try:
            return Key.from_private_key_file(path)
        except Exception:
            continue
    raise RuntimeError("Unable to load SSH private key (tried RSA/ECDSA/Ed25519)")

def _connect_ssh(public_dns: str) -> SSHClient:
    if not Path(PRIVATE_KEY_PATH).exists():
        raise FileNotFoundError(
            f"SSH key not found at {PRIVATE_KEY_PATH}. Set EC2_KEY_PATH env var or place labsuser.pem next to aws_word_count.py."
        )
    pkey = _load_private_key(PRIVATE_KEY_PATH)
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=public_dns, username=DEFAULT_SSH_USER, pkey=pkey, look_for_keys=False, allow_agent=False)
    return ssh

def _run_remote(ssh: SSHClient, command: str, desc: str) -> None:
    logger.info(f"Remote: {desc}")
    _, stdout, stderr = ssh.exec_command(command)
    code = stdout.channel.recv_exit_status()
    if code != 0:
        out = stdout.read().decode()
        err = stderr.read().decode()
        raise RuntimeError(f"Failed: {desc} (exit {code})\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    out = stdout.read().decode()
    if out:
        logger.info(out)

def _upload_files(ssh: SSHClient) -> None:
    home = f"/home/{DEFAULT_SSH_USER}"
    sftp = ssh.open_sftp()
    try:
        sftp.put(str(Path(__file__).parent / "install_hadoop.sh"), f"{home}/install_hadoop.sh")
        sftp.put(str(Path(__file__).parent / "install_spark.sh"), f"{home}/install_spark.sh")
        sftp.put(str(Path(__file__).parent / "word_count.py"), f"{home}/word_count.py")
        req = Path(__file__).parent / "requirements.txt"
        if req.exists():
            sftp.put(str(req), f"{home}/requirements.txt")
    finally:
        sftp.close()

def _run_sequence(ssh: SSHClient) -> None:
    home = f"/home/{DEFAULT_SSH_USER}"
    _run_remote(ssh, "bash -lc 'sudo apt-get update -y'", "apt update")
    _run_remote(ssh, "bash -lc 'sudo apt-get install -y wget curl tar openjdk-21-jdk python3-venv python3-pip'", "install deps")
    _run_remote(ssh, f"bash -lc 'cd {home} && sed -i " + '"s/\\r$//"' + " install_hadoop.sh install_spark.sh && chmod +x install_*.sh'", "normalize scripts")
    _run_remote(ssh, f"bash -lc 'cd {home} && ./install_hadoop.sh'", "install Hadoop")
    _run_remote(ssh, f"bash -lc 'cd {home} && ./install_spark.sh'", "install Spark")
    _run_remote(ssh, f"bash -lc 'cd {home} && rm -f hadoop-*.tar.gz spark-*.tgz || true'", "cleanup archives")
    _run_remote(ssh, f"bash -lc 'cd {home} && python3 -m venv venv && source venv/bin/activate && python -m pip install -U pip setuptools wheel'", "create venv")
    _run_remote(ssh, f"bash -lc 'cd {home} && source venv/bin/activate && pip install -r requirements.txt'", "pip install requirements")
    run_cmd = (
        f"bash -lc 'cd {home} && source venv/bin/activate && "
        "env HADOOP_HOME=/usr/local/hadoop SPARK_HOME=/usr/local/spark "
        "PATH=\"$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/usr/local/spark/sbin\" "
        "HADOOP_CLIENT_OPTS=\"-Dfs.defaultFS=file:/// -Dmapreduce.framework.name=local -Ddfs.replication=1\" "
        "MPLBACKEND=Agg python3 word_count.py'"
    )
    _run_remote(ssh, run_cmd, "run word_count.py")

def _download_results(ssh: SSHClient) -> None:
    """Download result images from the instance into a local aws_wordcount_results folder."""
    home = f"/home/{DEFAULT_SSH_USER}"
    local_dir = (Path(__file__).resolve().parent.parent / "aws_wordcount_results")
    local_dir.mkdir(parents=True, exist_ok=True)

    remote_files = [
        f"{home}/execution_times_comparison.png",
        f"{home}/execution_times_cloud_points.png",
    ]

    sftp = ssh.open_sftp()
    try:
        for remote in remote_files:
            filename = Path(remote).name
            dest = local_dir / filename
            try:
                sftp.get(remote, str(dest))
                logger.info(f"Downloaded {remote} -> {dest}")
            except FileNotFoundError:
                logger.warning(f"Result file not found on instance: {remote}")
    finally:
        sftp.close()

def main():
    ami = get_ami_id()
    vpc_id = get_default_vpc_id()
    sg_id = get_security_group_id(vpc_id, name="default")
    subnet_id = get_one_default_subnet(vpc_id)

    allow_my_ip_all(sg_id)
    inst = create_instance(ami, subnet_id, sg_id)
    wait_running_ok(inst)

    logger.info(f"Instance ready at {inst.public_dns_name}")
    ssh = _connect_ssh(inst.public_dns_name)
    try:
        _upload_files(ssh)
        _run_sequence(ssh)
        _download_results(ssh)
    finally:
        ssh.close()
    logger.info("Completed: ran word_count.py on single-node Ubuntu instance")


if __name__ == "__main__":
    main()