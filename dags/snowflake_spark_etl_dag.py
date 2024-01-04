from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators import dataproc
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime

# Fetching Dynamically accepted parameters
dynamic_configs = Variable.get("input_parameters", deserialize_json=True)
CLUSTER_NAME = dynamic_configs.get("CLUSTER_NAME")
PROJECT_ID = dynamic_configs.get("PROJECT_ID")
REGION = dynamic_configs.get("REGION")
CLUSTER_CONFIG = dynamic_configs.get("CLUSTER_CONFIG")
spark_main_file = dynamic_configs.get("spark_main_file")
sf_config = dynamic_configs.get("sf_config")
file_date = dynamic_configs.get("file_date")
if file_date is None:
    file_date = datetime.now().strftime("%Y%m%d")

if CLUSTER_CONFIG is None:
    CLUSTER_CONFIG = {
        "master_config":{
            "num_instances":1,
            "machine_type_uri":"n1-standard-2",
            "disk_config":{
                "boot_disk_type":"pd-standard",
                "boot_disk_size_gb":50
            }
        },
        "worker_config":{
            "num_instances":2,
            "machine_type_uri":"n1-standard-2",
            "disk_config":{
                "boot_disk_type":"pd-standard",
                "boot_disk_size_gb":50
            }
        },
        "software_config":{
            "image_version":"2.1-debian11"
        }
    }

if spark_main_file is None:
    spark_main_file = "gs://project-snowflake/snowflake_spark_main.py"

pyspark_job_arguments = { "spark_main_file":spark_main_file}


default_args = {
    "owner":"amitS",
    "depends_on_past":False,
    "email_on_failure":True,
    "emails":["sangwanamit@gmail.com"],
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(minutes=3),
    "schedule_interval": "30 9 * * *",
    "start_date": days_ago(1),
    "catchup":False
}


with DAG("project_snowflake", default_args=default_args):
    
    create_cluster = dataproc.DataprocCreateClusterOperator(
        task_id="cluster_creation",
        project_id= PROJECT_ID,
        cluster_name= CLUSTER_NAME,
        region= REGION,
        cluster_config= CLUSTER_CONFIG,
    )
    

    check_orders_file = GCSObjectsWithPrefixExistenceSensor(
        task_id = "orders_file_existance",
        bucket = "snowflake_621",
        prefix = f"orders_data/orders_{file_date}.csv",
        poke_interval=300,
        timeout=60*60*3
    )


    submit_spark_job = dataproc.DataprocSubmitPySparkJobOperator(
        task_id = "submit_spark_job",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME,
        main = pyspark_job_arguments["spark_main_file"],
        arguments = [f"--sf_config={sf_config}",
                     f"--file_date={file_date}"]
    )
    
    
    archive_orders_file = GCSToGCSOperator(
        task_id="archive_orders_file_after_transformation",
        source_bucket="snowflake_621",
        source_object=f"orders_data/orders_{file_date}.csv",
        destination_bucket="snowflake_621",
        destination_object="archived/orders_data/",
        move_object=True
    )


    delete_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id="cluster_deletion",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE
    )
    

    check_orders_file >> create_cluster >> submit_spark_job >> archive_orders_file >> delete_cluster
    
