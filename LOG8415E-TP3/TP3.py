import boto3
import logging
import os
import paramiko

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ec2 = boto3.resource("ec2")
ec2c = boto3.client("ec2")
NUMBER_OF_WORKERS = 2
KEY_PAIR_NAME = "labsuser"
HOME_DIR = os.path.expanduser("~")
PRIVATE_KEY_PATH = os.path.join(HOME_DIR, "labsuser.pem")
INSTANCE_USER = "ubuntu"


def create_vpc() -> str:
    """
    Creates a new VPC and returns its ID.
    """
    logger.info("Creating a new VPC")
    vpc = ec2c.create_vpc(CidrBlock="10.0.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2c.create_tags(Resources=[vpc_id], Tags=[{"Key": "Name", "Value": "tp3_vpc"}])
    logger.info(f"Created VPC: {vpc_id}")
    return vpc_id


def get_default_security_group_id(vpc_id: str, name: str = "default") -> str:
    """
    Returns the security group ID for the given VPC and group name.
    """
    resp = ec2c.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": [name]},
        ]
    )

    if not resp["SecurityGroups"]:
        raise RuntimeError(f"Cannot find security group {name} in VPC {vpc_id}")

    sg_id = resp["SecurityGroups"][0]["GroupId"]
    logger.info(f"found security group {name}: {sg_id}")

    return sg_id


def create_inbound_rule_to_allow_all_trafic(sg_id):
    """Create an inbound rule to allow all traffic in the specified security group."""
    logger.info(f"Creating inbound rule to allow all traffic in security group {sg_id}")
    ec2c.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 0,
                "ToPort": 65535,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    logger.info(f"Creation completed")


def create_subnet(vpc_id, name, availability_zone, cidr_block):
    """Create a private subnet in the specified VPC."""
    logger.info(f"Creating private subnet in VPC {vpc_id}")
    subnet = ec2c.create_subnet(
        CidrBlock=cidr_block, VpcId=vpc_id, AvailabilityZone=availability_zone
    )
    subnet_id = subnet["Subnet"]["SubnetId"]
    ec2c.create_tags(Resources=[subnet_id], Tags=[{"Key": "Name", "Value": name}])
    logger.info(f"Created subnet {subnet_id} in VPC {vpc_id}")
    return subnet_id


def create_gateway(vpc_id):
    """Create an internet gateway and attach it to the specified VPC."""
    logger.info(f"Creating internet gateway for VPC {vpc_id}")
    igw = ec2c.create_internet_gateway()
    igw_id = igw["InternetGateway"]["InternetGatewayId"]
    ec2c.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    ec2c.create_tags(Resources=[igw_id], Tags=[{"Key": "Name", "Value": "tp3_igw1"}])
    logger.info(f"Created and attached internet gateway {igw_id} to VPC {vpc_id}")
    return igw_id


def create_public_route_table(vpc_id):
    """Create a public route table in the specified VPC."""
    logger.info(f"Creating public route table in VPC {vpc_id}")
    route_table = ec2c.create_route_table(VpcId=vpc_id)
    rt_id = route_table["RouteTable"]["RouteTableId"]
    ec2c.create_tags(
        Resources=[rt_id], Tags=[{"Key": "Name", "Value": "tp3_public_rt1"}]
    )
    logger.info(f"Created public route table {rt_id} in VPC {vpc_id}")
    return rt_id


def create_route_to_internet_for_public_subnet(gateway_id, public_route_table_id):
    """Create a route to the internet for the public subnet."""
    logger.info(f"Creating route to internet for public subnet")
    ec2c.create_route(
        RouteTableId=public_route_table_id,
        DestinationCidrBlock="0.0.0.0/0",
        GatewayId=gateway_id,
    )


def create_inbound_route(gateway_id, public_route_table_id):
    """Create an inbound route for the internet gateway."""
    logger.info(f"Creating inbound route for internet gateway {gateway_id}")
    ec2c.create_route(
        RouteTableId=public_route_table_id,
        DestinationCidrBlock="0.0.0.0/0",
        GatewayId=gateway_id,
    )


def create_nat_gateway(subnet_id: str) -> str:
    """Create a NAT gateway in the specified VPC."""
    logger.info(f"Creating NAT gateway for subnet {subnet_id}")
    # Allocate an Elastic IP for the NAT gateway
    eip = ec2c.allocate_address(Domain="vpc")
    eip_allocation_id = eip["AllocationId"]
    logger.info(f"Allocated Elastic IP {eip_allocation_id} for NAT gateway")

    # Create the NAT gateway
    nat_gateway = ec2c.create_nat_gateway(
        SubnetId=subnet_id, AllocationId=eip_allocation_id
    )
    nat_gateway_id = nat_gateway["NatGateway"]["NatGatewayId"]
    ec2c.create_tags(
        Resources=[nat_gateway_id],
        Tags=[{"Key": "Name", "Value": "private_subnet_gateway"}],
    )
    logger.info(f"Created NAT gateway {nat_gateway_id} in subnet {subnet_id}")
    return nat_gateway_id


def create_private_route_table(vpc_id):
    """Create a private route table in the specified VPC."""
    logger.info(f"Creating private route table in VPC {vpc_id}")
    route_table = ec2c.create_route_table(VpcId=vpc_id)
    rt_id = route_table["RouteTable"]["RouteTableId"]
    ec2c.create_tags(
        Resources=[rt_id], Tags=[{"Key": "Name", "Value": "tp3_private_rt1"}]
    )
    logger.info(f"Created private route table {rt_id} in VPC {vpc_id}")
    return rt_id


# Add Route to Private Route Table 1 for NAT Gateway (Outbound internet access for private subnet)
def create_outband_rule(nat_gateway_id, private_route_id):
    """Create an outbound rule for the NAT gateway."""
    logger.info(f"Creating outbound rule for NAT gateway {nat_gateway_id}")
    ec2c.create_route(
        RouteTableId=private_route_id,
        DestinationCidrBlock="0.0.0.0/0",
        NatGatewayId=nat_gateway_id,
    )


def configure_public_subnet_network(vpc_id, public_subnet_id):
    """Configure the public subnet network."""
    logger.info(f"Configuring public subnet network for VPC {vpc_id}")
    igw_id = create_gateway(vpc_id)
    public_route_table_id = create_public_route_table(vpc_id)
    create_inbound_route(igw_id, public_route_table_id)
    ec2c.associate_route_table(
        RouteTableId=public_route_table_id, SubnetId=public_subnet_id
    )
    create_route_to_internet_for_public_subnet(igw_id, public_route_table_id)


def configure_private_subnet_network(vpc_id, public_subnet_id, private_subnet_id):
    """Configure the private subnet network."""
    logger.info(f"Configuring private subnet network for VPC {vpc_id}")
    nat_gateway_id = create_nat_gateway(public_subnet_id)
    private_route_table_id = create_private_route_table(vpc_id)
    ec2c.associate_route_table(
        RouteTableId=private_route_table_id, SubnetId=private_subnet_id
    )
    logger.info("Wait for the NAT gateway to become available")
    waiter = ec2c.get_waiter("nat_gateway_available")
    waiter.wait(NatGatewayIds=[nat_gateway_id])
    create_outband_rule(nat_gateway_id, private_route_table_id)


def get_ami_id():
    """
    Retrieves the latest Ubuntu Server 24.04 LTS AMI ID using SSM.
    """
    logger.info("Retrieving latest Ubuntu Server 24.04 LTS AMI ID...")
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(
        Name="/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
    )
    ami_id = response["Parameter"]["Value"]
    logger.info(f"Retrieved AMI ID: {ami_id}")
    return ami_id


def create_instances(public_subnet_id, private_subnet_id, sg_id, ami_id) -> dict:
    instance_names = ["gatekeeper", "proxy", "manager"] + [
        f"worker{i+1}" for i in range(NUMBER_OF_WORKERS)
    ]
    subnet_id_by_instance_name = {
        "gatekeeper": public_subnet_id,
        "proxy": private_subnet_id,
        "manager": private_subnet_id,
        **{f"worker{i+1}": private_subnet_id for i in range(NUMBER_OF_WORKERS)},
    }
    instances_by_name = {
        "gatekeeper": "t2.large",
        "proxy": "t2.large",
        "manager": "t2.micro",
        **{f"worker{i+1}": "t2.micro" for i in range(NUMBER_OF_WORKERS)},
    }
    for instance_name in instance_names:
        logger.info(f"Creating instance {instance_name}...")
        instances_by_name[instance_name] = ec2.create_instances(
            ImageId=ami_id,
            InstanceType=instances_by_name[instance_name],
            KeyName=KEY_PAIR_NAME,
            MinCount=1,
            MaxCount=1,
            NetworkInterfaces=[
                {
                    "SubnetId": subnet_id_by_instance_name[instance_name],
                    "DeviceIndex": 0,
                    "AssociatePublicIpAddress": instance_name == "gatekeeper",
                    "Groups": [sg_id],
                }
            ],
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": instance_name}],
                }
            ],
        )[0]
    return instances_by_name


def wait_until_running(instance):
    """
    Waits until the given EC2 instance is in the 'running' state.
    """
    logger.info(f"Waiting for instance {instance.id} to be running...")
    instance.wait_until_running()
    instance.reload()
    logger.info(
        f'Instance {next((tag["Value"] for tag in instance.tags if tag["Key"] == "Name"), "unknown")} is running at {instance.public_dns_name}'
    )


def wait_until_passed_status_checks(instance):
    """
    Waits until the given EC2 instance passes AWS status checks.
    """
    logger.info(f"Waiting for instance {instance.id} to pass status checks...")
    waiter = ec2c.get_waiter("instance_status_ok")
    waiter.wait(InstanceIds=[instance.id])
    logger.info(
        f'Instance {next((tag["Value"] for tag in instance.tags if tag["Key"] == "Name"), "unknown")} is ready to use.'
    )


def wait_for_instances(instances):
    """
    Waits for all created EC2 instances to be running and pass status checks.
    """
    logger.info("Waiting for instances to be running...")
    for instance in instances:
        wait_until_running(instance)
        wait_until_passed_status_checks(instance)


def init_instance_gatekeeper(gatekeeper_ip, proxy_ip):
    """
    Initialize the gatekeeper instance.
    """
    instance_name = "gatekeeper"
    os.system(f"chmod 400 {PRIVATE_KEY_PATH}")
    logger.info(f"Copying app files to {instance_name}...")
    os.system(
        f"scp -r -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null app ubuntu@{gatekeeper_ip}:/home/ubuntu/"
    )
    logger.info(f"Copied app files to {instance_name}.")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=gatekeeper_ip, username=INSTANCE_USER, key_filename=PRIVATE_KEY_PATH
    )
    logger.info(f"Starting gatekeeper application on {instance_name}...")
    stdin, stdout, stderr = ssh_client.exec_command(
        f"chmod +x /home/ubuntu/app/install_dependencies.sh && sudo /home/ubuntu/app/install_dependencies.sh {proxy_ip}"
    )
    exit_status = stdout.channel.recv_exit_status()
    os.system(
        f"ssh -i {PRIVATE_KEY_PATH} "
        f"-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f"ubuntu@{gatekeeper_ip} "
        f"'export PROXY_IP={proxy_ip}; cd app; rm -f proxy.py; nohup uvicorn gatekeeper:app --host 0.0.0.0 --port 8000 "
        f"> gatekeeper.log 2>&1 < /dev/null &'"
    )
    logger.info("Gatekeeper application started.")
    ssh_client.close()


def init_instance_proxy(instances_ip_by_name):
    """
    Initialize the proxy instance.
    """
    instance_name = "proxy"
    gatekeeper_ip = instances_ip_by_name["gatekeeper"]
    proxy_ip = instances_ip_by_name["proxy"]
    master_ip = instances_ip_by_name["manager"]
    worker_ip_separated_by_comma = ",".join(
        [instances_ip_by_name[f"worker{i+1}"] for i in range(NUMBER_OF_WORKERS)]
    )
    logger.info(f"Copying app files to {instance_name}...")
    os.system(
        f'scp -r -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o "ProxyCommand=ssh -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ubuntu@{gatekeeper_ip}" app ubuntu@{proxy_ip}:'
    )
    logger.info(f"Copied app files to {instance_name}.")
    ssh_client = ssh_to_private_instance(gatekeeper_ip, proxy_ip)
    logger.info(f"Starting proxy application on {instance_name}...")
    stdin, stdout, stderr = ssh_client.exec_command(
        f"chmod +x /home/ubuntu/app/install_dependencies.sh && sudo /home/ubuntu/app/install_dependencies.sh"
    )
    exit_status = stdout.channel.recv_exit_status()
    os.system(
        f"ssh -i {PRIVATE_KEY_PATH} "
        f"-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f'-o "ProxyCommand=ssh -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ubuntu@{gatekeeper_ip}" '
        f"ubuntu@{proxy_ip} "
        f"\"echo 'export MASTER_IP={master_ip}' >> ~/.bashrc; "
        f"echo 'export WORKER_IPS={worker_ip_separated_by_comma}' >> ~/.bashrc; "
        f"source ~/.bashrc; "
        f"export MASTER_IP={master_ip}; export WORKER_IPS={worker_ip_separated_by_comma}; "
        f"cd app; rm -f gatekeeper.py; "
        f"nohup uvicorn proxy:app --host 0.0.0.0 --port 8000 > proxy.log 2>&1 < /dev/null &\""
    )
    logger.info("Proxy application started.")
    ssh_client.close()


def ssh_to_private_instance(gatekeeper_ip, private_instance_ip):
    jump_ssh = paramiko.SSHClient()
    jump_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    jump_ssh.connect(
        hostname=gatekeeper_ip, username=INSTANCE_USER, key_filename=PRIVATE_KEY_PATH
    )

    jump_transport = jump_ssh.get_transport()

    dest_addr = (private_instance_ip, 22)
    local_addr = ("127.0.0.1", 0)

    channel = jump_transport.open_channel("direct-tcpip", dest_addr, local_addr)

    target_ssh = paramiko.SSHClient()
    target_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    target_ssh.connect(
        hostname=private_instance_ip,
        username=INSTANCE_USER,
        key_filename=PRIVATE_KEY_PATH,
        sock=channel,
    )

    return target_ssh


def configure_master_db_node(gate_keeper_ip, master_ip: list) -> tuple:
    os.system(
        f'scp -r -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o "ProxyCommand=ssh -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ubuntu@{gate_keeper_ip}" mysql ubuntu@{master_ip}:'
    )
    logger.info(f"Configuring master DB node at {master_ip}...")
    ssh_client = ssh_to_private_instance(gate_keeper_ip, master_ip)
    ssh_client.exec_command(
        f"chmod +x /home/ubuntu/mysql/install_mysql.sh && chmod +x /home/ubuntu/mysql/configure_master.sh"
    )
    stdin, stdout, stderr = ssh_client.exec_command(
        "sudo /home/ubuntu/mysql/install_mysql.sh"
    )
    logger.info(stdout.read().decode())
    logger.error(stderr.read().decode())

    # Retrieve the master log file and position
    stdin, stdout, stderr = ssh_client.exec_command(
        "sudo /home/ubuntu/mysql/configure_master.sh"
    )
    lines = stdout.readlines()
    lines = [l.strip() for l in lines if l.strip()]
    bin_file = lines[-2]
    position = lines[-1]
    ssh_client.close()
    logger.info(
        f"Master DB node at {master_ip} configured successfully with bin file {bin_file} and position {position}."
    )

    return bin_file, position


def configure_worker_db_node(gatekeeper_ip, worker_ip, master_ip, bin_file, position):
    logger.info(
        f"Configuring worker DB node at {worker_ip} to replicate from master at {master_ip}..."
    )
    os.system(
        f'scp -r -i {PRIVATE_KEY_PATH}  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o "ProxyCommand=ssh -i {PRIVATE_KEY_PATH} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ubuntu@{gatekeeper_ip}" mysql ubuntu@{worker_ip}:'
    )
    ssh_client = ssh_to_private_instance(gatekeeper_ip, worker_ip)
    ssh_client.exec_command(
        f"chmod +x /home/ubuntu/mysql/install_mysql.sh && chmod +x /home/ubuntu/mysql/configure_worker.sh"
    )
    stdin, stdout, stderr = ssh_client.exec_command(
        f"sudo /home/ubuntu/mysql/install_mysql.sh && sudo /home/ubuntu/mysql/configure_worker.sh {master_ip} {bin_file} {position}"
    )
    logger.info(stdout.read().decode())
    logger.error(stderr.read().decode())
    ssh_client.close()
    logger.info(f"Worker DB node at {worker_ip} configured successfully.")


def get_instances_ip(instances_by_name):
    logger.info("Retrieving instances IP addresses...")
    instances_ip_by_name = {}
    for instance_name, instance in instances_by_name.items():
        if instance_name == "gatekeeper":
            instances_ip_by_name[instance_name] = instance.public_ip_address
        else:
            instances_ip_by_name[instance_name] = instance.private_ip_address
    logger.info(f"Instances IP addresses: {instances_ip_by_name}")
    return instances_ip_by_name


def configure_instances(instances_ip_by_name):
    logger.info("Configuring instances...")
    gatekeeper_ip = instances_ip_by_name["gatekeeper"]
    master_ip = instances_ip_by_name["manager"]
    proxy_ip = instances_ip_by_name["proxy"]

    init_instance_gatekeeper(gatekeeper_ip, proxy_ip)
    init_instance_proxy(instances_ip_by_name)

    bin_file, position = configure_master_db_node(gatekeeper_ip, master_ip)

    for i in range(NUMBER_OF_WORKERS):
        worker_ip = instances_ip_by_name[f"worker{i+1}"]
        configure_worker_db_node(
            gatekeeper_ip, worker_ip, master_ip, bin_file, position
        )

    logger.info("All instances configured successfully.")


def main():
    try:
        ami_id = get_ami_id()
        vpc_id = create_vpc()
        default_sg_id = get_default_security_group_id(vpc_id)
        create_inbound_rule_to_allow_all_trafic(default_sg_id)
        public_subnet_id = create_subnet(
            vpc_id, "tp3_public_subnet", "us-east-1a", "10.0.0.0/24"
        )
        private_subnet_id = create_subnet(
            vpc_id, "tp3_private_subnet", "us-east-1a", "10.0.1.0/24"
        )
        configure_public_subnet_network(vpc_id, public_subnet_id)
        configure_private_subnet_network(vpc_id, public_subnet_id, private_subnet_id)
        instances_by_name = create_instances(
            public_subnet_id, private_subnet_id, default_sg_id, ami_id
        )
        wait_for_instances(instances_by_name.values())
        instances_ip_by_name = get_instances_ip(instances_by_name)
        configure_instances(instances_ip_by_name)

    except Exception as e:
        logger.error(f"Error occurred while creating resources: {e}")


if __name__ == "__main__":
    main()
