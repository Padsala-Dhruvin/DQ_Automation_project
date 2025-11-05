Pipeline Execution and Airflow Concept
The run_pipeline.py script serves as the final orchestrator. This project demonstrates not just the ability to write DQ checks, but also an understanding of workflow automation necessary for scalable Data Engineering.

Airflow (Job Orchestration) Mapping
In a production environment , this daily workflow would be managed by Apache Airflow.

Directed Acyclic Graph (DAG): The entire workflow (from connecting to the database to checking all functions) would be defined as a single DAG.

Tasks: Each distinct step in the run_pipeline.py execution is mapped to a dedicated Airflow Task:

Task 1: connect_db (Establishes the connection)

Task 2: check_null_keys

Task 3: check_invalid_range

Task 4: check_duplicates

Task Task 5: send_success_notification

Sequential Dependency: Airflow ensures that Task 3 cannot run until Task 2 has successfully completed, ensuring reliable, ordered execution.

Failure Handling: If any DQ Task (like check_invalid_range) fails (returns False), Airflow automatically stops the DAG execution, prevents subsequent tasks (like analysis or reporting) from running on bad data, and triggers a failure alert.
