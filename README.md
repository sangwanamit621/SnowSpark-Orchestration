# SnowSpark-Orchestration

## Overview
This project orchestrates a data processing workflow using Apache Airflow, Spark, Google Cloud Storage (GCS), and Snowflake. The workflow is designed to handle daily data updates, filtering completed orders, and updating a Snowflake target table with the latest information. The project leverages Apache Airflow for workflow scheduling and management, Spark for data processing, and Google Cloud Platform services for storage, cluster orchestration, and data transfer.

## Technologies Used
  * __Apache Airflow:__ Open-source platform for orchestrating complex workflows.
  * __Apache Spark:__ Distributed computing framework for processing large-scale data.
  * __Google Cloud Platform (GCP):__ Cloud services used for storage (GCS), cluster creation (Dataproc), and data transfer.
  * __Snowflake:__ Cloud-based data warehousing platform.

## Project Structure
The project consists of two main components:
1. **Airflow DAG (`snowflake_spark_etl_dag`):**
    * **Cluster Creation (`cluster_creation`):** Creates a Dataproc cluster for Spark job execution.
    * **File Existence Check (`orders_file_existance`):** Monitors the existence of the daily data file in GCS.
    * **Spark Job Submission (`submit_spark_job`):** Submits a PySpark job to process and update data using the provided Spark script.
    * **File Archival (`archive_orders_file_after_transformation`):** Archives the processed daily data file in GCS.
    * **Cluster Deletion (cluster_deletion):** Deletes the Dataproc cluster upon successful completion of the workflow.

2. **Spark Script (`snowflake_spark_main.py`):**
    * Reads data from the Snowflake target table (`completed_orders_data`).
    *  Reads daily data from the GCS bucket (`orders_data/orders_{file_date}.csv`).
    *  Filters completed orders and identifies returned products.
    *  Removes records for returned products from the Snowflake target table.
    *  Combines the filtered GCS data with the Snowflake target table.
    *  Overwrites the Snowflake target table with the updated data.

## Setup
1. **Airflow Setup:**
    *  Install and configure Apache Airflow.
    *  Copy the provided DAG script (`snowflake_spark_etl_dag.py`) into the Airflow DAGs directory.

2. **Spark Setup:**
    *  Ensure that you have Spark installed.
    *  Place the Spark script (`snowflake_spark_main.py`) in a location accessible to the Dataproc cluster.

3. **Jar Files for Spark to connect to Snowflake:**
    *  The below link of jar files works for `Scala 2.13, 2.12` and `Spark 3.4, 3.3, 3.2`
    *  [spark-snowflake_2.12-2.12.0-spark_3.3.jar](https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/)
    *  [snowflake-jdbc-3.13.30.jar](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.30/)
      
4. **Google Cloud Platform Setup:**
    *  Configure GCP credentials for Airflow.
    *  Set up a GCS bucket (snowflake_621) for storing daily data files.
    
5. **Airflow Variable Configuration:**
    *  Set the necessary parameters in the Airflow variables, including input_parameters for dynamic configuration.


## How to Run
1. **Airflow DAG Execution:**
    *  The DAG is scheduled to run daily at 09:30 AM (`"schedule_interval": "30 9 * * *"`).
    *  Manually trigger the DAG or let it run automatically based on the schedule.

2. **Spark Script Execution:**
    *  Upload the Spark script (`snowflake_spark_main.py`) to a GCS bucket.
    *  Provide the GCS path in the corresponding Airflow task (`submit_spark_job`).
    *  Ensure the necessary dependencies, such as Snowflake JDBC and Spark Snowflake connector JARs, are accessible by specifying them in the Spark configuration (`spark.jars`). Jars can also be uploaded into GCS bucket.

3. **Monitoring and Logs:**
    *  Monitor the DAG execution progress through the Airflow UI.
    *  Check Spark job logs for detailed information on data processing.
      
4. **Customization:**
    *  Customize Spark script parameters, GCS paths, Snowflake configurations, and DAG schedule based on your specific setup.
    
Feel free to modify the code and adapt it to your use case. For any issues or improvements, please open an issue or pull request on GitHub.

