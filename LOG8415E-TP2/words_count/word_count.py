import logging
import subprocess
import time
from datetime import datetime
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# the key is the file name and the value a list of NUMBER_OF_REPETITIONS execution times
hadoop_execution_times = {}
spark_execution_times = {}
linux_execution_times = {}

NUMBER_OF_REPETITIONS = 3
SOURCES_LIST = ["https://tinyurl.com/4vxdw3pa",
"https://tinyurl.com/kh9excea",
"https://tinyurl.com/dybs9bnk",
"https://tinyurl.com/datumz6m",
"https://tinyurl.com/j4j4xdw6",
"https://tinyurl.com/ym8s5fm4",
"https://tinyurl.com/2h6a75nk",
"https://tinyurl.com/vwvram8",
"https://tinyurl.com/weh83uyn"]

def create_folders():
    """Create input folder in HDFS"""
    logger.info("Creating input/ directory")
    subprocess.run("hdfs dfs -mkdir -p input/ && hdfs dfs -rm -f input/* && rm -rf output", shell=True, check=True)
    logger.info("Created input/ directory")

def download_sources_and_update_to_hdfs():
    """Download source files and upload them to HDFS input/ directory"""
    logger.info("Downloading source files and uploading to HDFS input/ directory")
    for url in SOURCES_LIST:
        file_name = url.split("/")[-1]
        logger.info(f"Downloading from {url}")
        subprocess.run(
            f"wget -q {url} && \
            hdfs dfs -put {file_name} input/ && \
                rm {file_name}",
            shell=True,
            check=True
        )
        logger.info(f"Downloaded {file_name} to HDFS input/ directory")
        
# Return execution time in milliseconds
def hadoop_word_count(file_path: str) -> float:
    """Run Hadoop word count job on the given file and return execution time in milliseconds"""
    file_name = file_path.split("/")[-1]
    logger.info(f"Running Hadoop word count on file {file_name}")
    start_time = time.perf_counter()
    subprocess.run(
        f"hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar wordcount {file_path} output",
        # && hadoop fs -cat output/part-r-00000 > output/hadoop_{file_name}_output.txt",   # Uncomment to save output
        shell=True,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    end_time = time.perf_counter()
    execution_time = (end_time - start_time) * 1000
    return execution_time
    
def spark_word_count(file_path: str) -> float:
    """Run Spark word count job on the given file and return execution time in milliseconds"""
    file_name = file_path.split("/")[-1]
    logger.info(f"Running Spark word count on file {file_name}")
    start_time = time.perf_counter()
    subprocess.run(
        f"spark-submit --master local[2] --class org.apache.spark.examples.JavaWordCount /usr/local/spark/examples/jars/spark-examples_2.13-4.0.1.jar {file_path} output",
        #  && hdfs dfs -cat output/part-00000 > output/spark_{file_name}_output.txt"    # Uncomment to save output
        shell=True,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    end_time = time.perf_counter()
    execution_time = (end_time - start_time) * 1000
    return execution_time

def linux_word_count(file_path: str) -> float:
    """Run Linux word count on the given file and return execution time in milliseconds"""
    file_name = file_path.split("/")[-1]
    logger.info(f"Running Linux word count on file {file_name}")
    start_time = time.perf_counter()
    subprocess.run(
        f"cat {file_path} | tr ' ' '\n' | sort | uniq -c | sort -nr > output/linux_{file_name}_output.txt",
        shell=True,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    end_time = time.perf_counter()
    execution_time = (end_time - start_time) * 1000
    return execution_time

def compare_hadoop_and_spark() -> tuple[dict, dict]:
    """Measure execution times for Hadoop and Spark word count jobs on all source files"""
    for source_url in SOURCES_LIST:
        file_name = source_url.split("/")[-1]
        file_path = f"input/{file_name}"
        hadoop_time = 0.0
        spark_time = 0.0
        logger.info(f"Measuring execution times for source: {file_name}")
        
        hadoop_execution_times[file_name] = []
        spark_execution_times[file_name] = []
        for _ in range(NUMBER_OF_REPETITIONS):
            hadoop_time = hadoop_word_count(file_path)
            spark_time = spark_word_count(file_path)
            hadoop_execution_times[file_name].append(hadoop_time)
            spark_execution_times[file_name].append(spark_time)
            subprocess.run(" rm -rf output", shell=True, check=True)
            
    logger.info("Completed measuring execution times")
    return hadoop_execution_times, spark_execution_times


def compare_hadoop_and_linux() -> tuple[dict, dict]:
    """Measure execution times for Hadoop and Linux word count jobs on all source files"""
    for source_url in SOURCES_LIST:
        file_name = source_url.split("/")[-1]
        file_path = f"input/{file_name}"
        hadoop_time = 0.0
        linux_time = 0.0
        logger.info(f"Measuring execution times for source: {file_name}")

        hadoop_execution_times[file_name] = []
        linux_execution_times[file_name] = []
        for _ in range(NUMBER_OF_REPETITIONS):
            hadoop_time = hadoop_word_count(file_path)
            linux_time = linux_word_count(file_path)
            hadoop_execution_times[file_name].append(hadoop_time)
            linux_execution_times[file_name].append(linux_time)
            subprocess.run(" rm -rf output", shell=True, check=True)

    logger.info("Completed measuring execution times")
    return hadoop_execution_times, linux_execution_times

def plot_execution_times(execution_times1: dict, execution_times2: dict, label1: str = "Hadoop", label2: str = "Spark"):
    """Plot execution times for two different systems"""
    import matplotlib.pyplot as plt
    import numpy as np

    file_names = list(execution_times1.keys())
    x = np.arange(len(file_names))
    width = 0.35

    avg_times1 = [sum(times) / len(times) for times in execution_times1.values()]
    avg_times2 = [sum(times) / len(times) for times in execution_times2.values()]

    fig, ax = plt.subplots()
    bars1 = ax.bar(x - width/2, avg_times1, width, label=label1)
    bars2 = ax.bar(x + width/2, avg_times2, width, label=label2)

    ax.set_ylabel('Execution Time (milliseconds)')
    ax.set_title('Execution Times by File and System')
    ax.set_xticks(x)
    ax.set_xticklabels(file_names, rotation=45, ha='right')
    ax.legend()

    plt.tight_layout()
    plt.savefig('execution_times_comparison.png')
    
def plot_cloud_points(execution_times1: dict, execution_times2: dict, label1: str = "Hadoop", label2: str = "Spark"):
    """Plot execution times for two different systems as cloud points"""
    import matplotlib.pyplot as plt

    file_names = list(execution_times1.keys())

    plt.figure(figsize=(10, 6))
    for file_name in file_names:
        times1 = execution_times1[file_name]
        times2 = execution_times2[file_name]
        plt.scatter([file_name]*len(times1), times1, label=f"{label1} - {file_name}", alpha=0.6)
        plt.scatter([file_name]*len(times2), times2, label=f"{label2} - {file_name}", alpha=0.6)

    plt.ylabel('Execution Time (milliseconds)')
    plt.title('Execution Times by File and System (Cloud Points)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('execution_times_cloud_points.png')

def is_running_on_aws() -> bool:
    """Check if the code is running on an AWS instance"""
    try:
        with open("/sys/hypervisor/uuid", "r") as f:
            uuid = f.read()
            return uuid.startswith("ec2")
    except FileNotFoundError:
        return False

def delete_downloaded_files():
    logger.info("Deleting downloaded files in input/")
    subprocess.run("hdfs dfs -rm input/*", shell=True, check=True)
    logger.info("Deleted downloaded files")

def main():
    create_folders()
    download_sources_and_update_to_hdfs()
    execution_time1 = {}
    execution_time2 = {} 
    if is_running_on_aws():
        hadoop_times, spark_times = compare_hadoop_and_spark()
        execution_time1 = hadoop_times
        execution_time2 = spark_times
        label1 = "Hadoop"
        label2 = "Spark"
    else:
        hadoop_times, linux_times = compare_hadoop_and_linux()
        execution_time1 = hadoop_times
        execution_time2 = linux_times
        label1 = "Hadoop"
        label2 = "Linux"

    plot_cloud_points(execution_time1, execution_time2, label1, label2)
    plot_execution_times(execution_time1, execution_time2, label1, label2)

    delete_downloaded_files()
    
if __name__ == "__main__":
    main()