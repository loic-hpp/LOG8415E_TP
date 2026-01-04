import boto3
import logging
import os
from paramiko import SSHClient
import paramiko

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NUMBER_OF_T2_MICRO_INSTANCE = 4
NUMBER_OF_T2_LARGE_INSTANCE = 4
IAM_PROFILE = "LabInstanceProfile"
PRIVATE_KEY_PATH = "labsuser.pem"
MAIN_CLUSTER_SCRIPT = "main_cluster"

ec2 = boto3.resource('ec2')
ec2c = boto3.client("ec2")
   
def init_instance(public_dns, instance_name, cluster_name):
    """
    Initializes a remote EC2 instance by copying application files, setting permissions, and running a bootstrap script.
    """
    # change chmod for .pem file
    os.system(f"chmod 400 {PRIVATE_KEY_PATH}")
    # copy app files
    logger.info(f"Copying app files to {instance_name}...")
    os.system(f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {PRIVATE_KEY_PATH} -r ./app ec2-user@{public_dns}:/home/ec2-user/")
    
    logger.info(f"Connecting to instance {instance_name} via SSH...")
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=public_dns, username="ec2-user", key_filename=PRIVATE_KEY_PATH)
    
    logger.info(f"Setting execute permission for bootstrap script on {instance_name}...")
    stdin, stdout, stderr = ssh.exec_command("chmod +x /home/ec2-user/app/bootstrap.sh")
    logger.info(stdout.read().decode())
    logger.error(stderr.read().decode())
    
    logger.info(f"Running bootstrap script on {instance_name}...")
    stdin, stdout, stderr = ssh.exec_command(f"/home/ec2-user/app/bootstrap.sh {instance_name} {cluster_name}")
    logger.info(stdout.read().decode())
    logger.error(stderr.read().decode())
    
    ssh.close()    
    
def init_cluster(instances, cluster_name):
    """
    Initializes all instances in a cluster by copying files and running bootstrap scripts.
    """
    for instance in instances:
        init_instance(instance.public_dns_name, next((tag["Value"] for tag in instance.tags if tag["Key"] == "Name"), "unknown"), cluster_name)

def init_load_balancer(public_dns):
    """
    Initializes the load balancer instance by copying files and running the bootstrap script.
    """
    # change chmod for .pem file
    os.system(f"chmod 400 {PRIVATE_KEY_PATH}")
    # copy load balancer files
    logger.info("Copying load balancer files...")
    os.system(f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {PRIVATE_KEY_PATH} -r ./lb ec2-user@{public_dns}:/home/ec2-user/")

    logger.info("Connecting to load balancer instance via SSH...")
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=public_dns, username="ec2-user", key_filename=PRIVATE_KEY_PATH)
    
    logger.info("Setting execute permission for load balancer bootstrap script...")
    stdin, stdout, stderr = ssh.exec_command("chmod +x /home/ec2-user/lb/bootstrap.sh")
    logger.info(stdout.read().decode())
    logger.error(stderr.read().decode())
    
    logger.info("Running load balancer bootstrap script...")
    stdin, stdout, stderr = ssh.exec_command("/home/ec2-user/lb/bootstrap.sh")
    logger.info(stdout.read().decode())
    logger.error(stderr.read().decode())
    
    ssh.close()

def create_my_ip_inbound_sg_rule(sg_id):
    """
    Adds an inbound rule to the security group to allow all traffic from your public IP.
    """
    my_ip = boto3.client('ec2').meta.endpoint_url  # fallback if needed
    try:
        my_ip = os.popen('curl -s https://checkip.amazonaws.com').read().strip()
        ec2c.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    'IpProtocol': '-1',
                    'FromPort': -1,
                    'ToPort': -1,
                    'IpRanges': [{'CidrIp': f'{my_ip}/32', 'Description': 'Allow all traffic from my IP'}]
                }
            ]
        )
        logger.info(f"Added inbound rule for all traffic from {my_ip}/32 to security group {sg_id}")
    except Exception as e:
        logger.warning(f"Could not add inbound rule for {my_ip}/32: {e}")
        exit(1)

# Create t2.micro instances
def create_t2_micro_instances(sg_id):
    """
    Creates t2.micro EC2 instances for cluster1 and appends them to the list.
    """
    logger.info("Creating t2.micro instances...")
    for i in range(NUMBER_OF_T2_MICRO_INSTANCE):
        instance_name = f"cluster1-t2-micro-instance-{i+1}"
        logger.info(f"Creating instance {instance_name}...")
        instance = ec2.create_instances(
            ImageId=image_load_balancer_id,
            InstanceType="t2.micro",
            KeyName=key_pair_name,
            MinCount=1,
            MaxCount=1,
            SubnetId=subnet_ids[0],
            SecurityGroupIds=[sg_id],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': instance_name},
                             {'Key': 'Cluster', 'Value': 'cluster1'}]
                }
            ]
        )[0]
        t2_micro_instances.append(instance)

# Create t2.large instances
def create_t2_large_instances(sg_id):
    """
    Creates t2.large EC2 instances for cluster2 and appends them to the list.
    """
    logger.info("Creating t2.large instances...")
    for i in range(NUMBER_OF_T2_LARGE_INSTANCE):
        instance_name = f"cluster2-t2-large-instance-{i+1}"
        logger.info(f"Creating instance {instance_name}...")
        instance = ec2.create_instances(
            ImageId=image_load_balancer_id,
            InstanceType="t2.large",
            KeyName=key_pair_name,
            MinCount=1,
            MaxCount=1,
            SubnetId=subnet_ids[1],
            SecurityGroupIds=[sg_id],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': instance_name},
                             {'Key': 'Cluster', 'Value': 'cluster2'}]
                }
            ]
        )[0]
        t2_large_instances.append(instance)
        
def create_load_balancer_instance(sg_id):
    """
    Creates the load balancer EC2 instance and appends it to the list.
    """
    logger.info("Creating load balancer instance...")
    instance_name = "load-balancer"
    instance = ec2.create_instances(
        ImageId=image_load_balancer_id,
        InstanceType="t2.large",
        KeyName=key_pair_name,
        MinCount=1,
        MaxCount=1,
        SubnetId=subnet_ids[1],
        SecurityGroupIds=[sg_id],
        IamInstanceProfile={'Name': IAM_PROFILE},  # Ensure this role exists with necessary permissions but by default it is present in AWS Academy Learner Lab
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': instance_name},
                         {'Key': 'Cluster', 'Value': 'load-balancer'}]
            }
        ]
    )[0]
    load_balancer_instance.append(instance)
    
def wait_until_running(instance):
    """
    Waits until the given EC2 instance is in the 'running' state.
    """
    logger.info(f"Waiting for instance {instance.id} to be running...")
    instance.wait_until_running()
    instance.reload()
    logger.info(f'Instance {next((tag["Value"] for tag in instance.tags if tag["Key"] == "Name"), "unknown")} is running at {instance.public_dns_name}')
    
def wait_until_passed_status_checks(instance):
    """
    Waits until the given EC2 instance passes AWS status checks.
    """
    logger.info(f"Waiting for instance {instance.id} to pass status checks...")
    waiter = ec2c.get_waiter('instance_status_ok')
    waiter.wait(InstanceIds=[instance.id])
    logger.info(f'Instance {next((tag["Value"] for tag in instance.tags if tag["Key"] == "Name"), "unknown")} is ready to use.')

def wait_for_instances():
    """
    Waits for all created EC2 instances to be running and pass status checks.
    """
    logger.info("Waiting for instances to be running...")
    for instance in t2_micro_instances + t2_large_instances + load_balancer_instance:
        wait_until_running(instance)
        wait_until_passed_status_checks(instance)

def get_default_vpc_id():
    """
    Returns the default VPC ID for the current AWS region.
    """
    resp = ec2c.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    vpcs = resp.get("Vpcs", [])
    if not vpcs:
        raise RuntimeError("No default VPC in this region.")
    vpc_id = vpcs[0]["VpcId"]
    logger.info(f"default VPC: {vpc_id}")
    return vpc_id

def get_two_default_subnets(vpc_id):
    """
    Returns two default subnets in the given VPC.
    """
    resp = ec2c.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    subnets = sorted(resp["Subnets"], key=lambda s: s["AvailabilityZone"])
    
    if len(subnets) < 2:
        raise RuntimeError("Need at least two subnets.")

    picked = [s["SubnetId"] for s in subnets[:2]]
    logger.info(f"using subnets: {picked}")
    
    return picked

def get_security_group_id(vpc_id, name='default'):
    """
    Returns the security group ID for the given VPC and group name.
    """
    resp = ec2c.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": [name]}
        ]
    )

    if not resp["SecurityGroups"]:
        raise RuntimeError(f"Cannot find security group {name} in VPC {vpc_id}")

    sg_id = resp["SecurityGroups"][0]["GroupId"]
    logger.info(f"found security group {name}: {sg_id}")

    return sg_id

def get_ami_id():
    """
    Retrieves the latest Amazon Linux 2023 AMI ID using SSM.
    """
    logger.info("Retrieving latest Amazon Linux 2023 AMI ID...")
    ssm = boto3.client('ssm')
    response = ssm.get_parameter(Name='/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64')
    return response['Parameter']['Value']
    
vpc_id = get_default_vpc_id()
key_pair_name = "labsuser"  
instance_type = "t2.micro"
sg_id = get_security_group_id(vpc_id, name="default")
subnet_ids = get_two_default_subnets(vpc_id)
image_load_balancer_id = ""

t2_micro_instances = []
t2_large_instances = []
load_balancer_instance = []


def main():
    """
    Main entry point for resource creation and initialization.
    """
    try:
        global image_load_balancer_id
        image_load_balancer_id = get_ami_id()
        create_my_ip_inbound_sg_rule(sg_id)
        create_t2_micro_instances(sg_id)
        create_t2_large_instances(sg_id)
        create_load_balancer_instance(sg_id)
        wait_for_instances()
        init_load_balancer(load_balancer_instance[0].public_dns_name)
        init_cluster(t2_micro_instances, "cluster1")
        init_cluster(t2_large_instances, "cluster2")
        logger.info("Resources created successfully.")
        
    except Exception as e:
        logger.error(f"Error occurred while creating resources: {e}")

if __name__ == "__main__":
    main()