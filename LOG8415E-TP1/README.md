# TP1 – LOG8415E – Polytechnique Montréal

This project is the first practical assignment (TP1) for the course **LOG8415E** at **Polytechnique Montréal**.

## 🧠 Objective

The goal is to create a Python script (`TP1.py`) that deploys an AWS infrastructure composed of:

- An **Application Load Balancer (ALB)**
- **Two Target Groups**:
  - `cluster1`: 4 EC2 instances of type `t2.micro`
  - `cluster2`: 4 EC2 instances of type `t2.large`

Each cluster runs a FastAPI server (already copied into the AMI used). You may ask the owner to make the AMI public if needed.

---

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
- ![1757173724983](image/README/1757173724983.png)
- Verify that you are logged by running:
```bash
aws sts get-caller-identity | tee
```

### 2. Configure user profile AWS region

- Open the AWS config file:
```bash
nano ~/.aws/config
```
- Add or update the following lines to set the default region:
```
[default]
region = us-east-1
```
- Save and close the file.

### 3. Download the default key pair

In your learner lab page download the default key pair as follow<br/>
![1758075212856](image/README/1758075212856.png)<br/>
> Then copy it into the project root directory and apply correct rights with command:
```bash
chmod 400 labsuser.pem
```
---

### 4. Create a new key pair (if you do not have `labsuser.pem`)

If you do not have a key pair named `labsuser` in your AWS account, create one using the AWS CLI:

```bash
aws ec2 create-key-pair --region us-east-1 --key-name labsuser --query 'KeyMaterial' --output text > labsuser.pem
```

This command creates a new key pair named `labsuser` with the private key as `labsuser.pem` in your aws account.

## 🚀 Deployment

Run the script to deploy the infrastructure:

```bash
python3 TP1.py
```

## 📊 Benchmarking
After the infrastructure is healthy, retrieve your Load Balancer DNS name, then run the benchmark tests:
```bash
python3 benchmark_cluster.py http://<load_balancer_dns>:8000/cluster1 > benchmark_result_cluster1.log
python3 benchmark_cluster.py http://<load_balancer_dns>:8000/cluster2 > benchmark_result_cluster2.log
```

## ✅ Completion
You have now completed the practical portion of TP1. Move to analysis part 