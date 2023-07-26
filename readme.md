# Assumptions:

* The host/machine where the program runs has system time in BST

# Schedule:
* To schedule the program for every Friday 8AM, a cron job can be added with below expression:


    "0 8 * * 5  python <project-directory>/src/start_etl_run.py"

where, <project-directory> is the actual path where the repository is copied/cloned.

* It can be scheduled by orchestrators like Apache Airflow or Azure Data Factory (using Databricks as compute).



