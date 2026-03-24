"""
My First DAG
============
Complete this file to create your first Airflow DAG.

Your DAG should:
1. Print a start message (BashOperator)
2. Process some data (PythonOperator)
3. Generate a report (BashOperator)
4. Print an end message (PythonOperator)
"""

# ============================================================
# Imports
# ============================================================

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


# ============================================================
# Python Functions
# ============================================================

def process_data():
    """
    It should:
    1. Print "Processing data..."
    2. Simulate processing by printing record counts
    3. Return a dictionary with status information
    """
    print("Processing data...")
    print("Records read: 100")
    print("Records processed: 100")

    return {"records_processed": 100, "status": "success"}


def generate_summary():
    """
    It should:
    1. Print a summary message
    2. Print the current timestamp
    3. Return a success message
    """
    print("Pipeline execution complete!")
    print(f"Completed at: {datetime.now()}")

    return "Pipeline finished successfully"


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="my_first_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "beginner"],
) as dag:

    # ============================================================
    # Tasks
    # ============================================================

    # Start task
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Pipeline starting at $(date)'"
    )

    # Process task
    process = PythonOperator(
        task_id="process",
        python_callable=process_data
    )

    # Report task
    report = BashOperator(
        task_id="report",
        bash_command="echo 'Generating report...'"
    )

    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=generate_summary
    )

    # ============================================================
    # Dependencies
    # ============================================================

    start >> process >> report >> end