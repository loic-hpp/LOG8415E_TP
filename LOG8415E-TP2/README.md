# LOG8415E-TP2

This project is the second practical assignment (TP2) for the course **LOG8415E** at **Polytechnique Montréal**.

## 🛠️ Environment Configuration

Before proceeding, set up a Python virtual environment and install the required dependencies:

```bash
python3 -m venv ~/awscli-venv
source ~/awscli-venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## ⚙️ Setup Instructions

### 1. Connect to AWS CLI

Follow the instructions shown in the image below to configure your local machine with AWS CLI access.
- Run the command 
```bash
nano ~/.aws/credentials
```
- Ajust parameter for the one found in your aws student console
- ![1757173724983](images/README/1757173724983.png)
- Verify that you are logged by running:
```bash
aws sts get-caller-identity | tee
```

### 2. Create a new key pair (if you do not have `labsuser` key pair)

If you do not have a key pair named `labsuser` in your AWS account, create one using the AWS CLI:

```bash
aws ec2 create-key-pair --region us-east-1 --key-name labsuser --query 'KeyMaterial' --output text > labsuser.pem
```

This command creates a new key pair named `labsuser` with the private key as `labsuser.pem` in your aws account.

If the key pair `labuser` already exists and it doesn't map to your AWS acocunt, you can delete the key pair with the following comamnd :

```bash
aws ec2 delete-key-pair --region us-east-1 --key-name labsuser
```

### 3. Update the `labsuser.pem` privte key permissions

```bash
chmod 400 labsuser.pem
```

### 4. Run the script to run Spark and Hadoop on AWS 

```bash
python aws_word_count.py
```

## AWS MapReduce

This project includes a small, modular and pluggable MapReduce for AWS. In short:

- The client reads a local input file, deploys the infrastructure (provisioning EC2 instances) and submits the job payload to the orchestrator.
- The orchestrator distributes input lines to mapper services, collects mapped key/value pairs, sorts and partitions them, then dispatches partitions to the reducer services and aggregates final results.
- Services are split into small FastAPI processes: mapper, reducer, partitioner and orchestrator. Each service runs on its own EC2 instance (by default) and exposes a simple HTTP API (including /health endpoints).
- `infrastructure_provisioning.py` creates instances, uploads code, starts services, and returns the runtime endpoints the client uses to submit jobs.
- `mapreduce_request.py` sends a mapreduce job for the dataset to the orchestrator and retrieves the result.
- `bootstrap_and_run.py` runs `infrastructure_provisioning` and `mapreduce_request`. Used to run everything if aws infrastructure isn't initialized.

This arrangement makes it easy to test different algorithms (see `algorithms/`), scale the number of mappers/reducers, and observe the MapReduce pipeline end-to-end.

### Setup

#### aws_config.json

Create an `aws_config.json` configuration file under the mapreduce/configs directory with the following values :

```json
{
    "region": "us-east-1",
    "ami_id": "ami-0c94855ba95c71c99",
    "instance_type": "t2.micro",
    "key_name": "labsuser",
    "key_path": "./labsuser.pem",
    "security_group_name": "mapreduce-tp2-sg",
    "subnet_id": null
}
```

Adjust `key_path`, `region`, `subnet_id` or other fields as needed for your environment.

#### mapreduce_config.json

Create an `mapreduce_config.json` configuration file under the mapreduce/configs directory with the following values :

```json
{
  "algorithm": "<wordcount | friend_recommandation>",
  "input_file": "<file_name>",
  "num_reducers": 3,
  "num_mappers": 2,
  "verbose": false
}
```

Adjust fields as needed for your environment.