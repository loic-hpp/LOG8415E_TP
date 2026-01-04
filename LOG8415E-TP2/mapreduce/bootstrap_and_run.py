from infrastructure_provisioning import deploy_and_get_config
from mapreduce_client import run_map_reduce

if __name__ == "__main__":
    # 1. Deploy infrastructure and get runtime config
    deployed = None
    try:
        deployed = deploy_and_get_config()
    except Exception as e:
        raise RuntimeError(f"Failed to import deploy_and_get_config from TP2.py: {e}")

    if not deployed:
        raise RuntimeError("Deployed config not available. Please deploy the infrastructure first.")

    # 2. Run MapReduce job with the deployed configuration
    run_map_reduce(deployed)