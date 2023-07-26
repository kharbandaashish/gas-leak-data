# Assumptions/Important Points:
* I have used pyspark as I am more comfortable with it. Same results can also be achieved by using pandas
* Records in leaks_table can be added or updated
    * New records can be identified using column "create_date"
    * Updated records can be identified using column "modified_date"
* Uperts are not supported in sqlite, so I've used complex conditions to create final output with the help of updates,
  appends and existing records. In real situation, I would recommend using delta format to support upserts
* The host/machine where the program runs has system time in BST, only for scheduling purpose if using cron
* In real world scenario, I would have clarified the requirments before assuming anything
* Some log files have been added purposely to show how they actually look, they can be ignored with the .gitignore file
# Schedule:

* To schedule the program for every Friday 8AM, a cron job can be added with below expression:

    "0 8 * * 5  python project-directory/dataeng-ak/src/start_etl_run.py"

where, project-directory is the actual path where the repository is copied/cloned.

* It can be scheduled by orchestrators like Apache Airflow or Azure Data Factory (using Databricks as compute).

* To run it using spark-submit, below command can be used:

    "spark-submit --packages org.xerial:sqlite-jdbc:3.42.0.0 project-directory/dataeng-ak/src/start_etl_run.py"


# Enhancement scope:
* Use of delta format to support upserts
* Use of linters like mypy, black, isort, etc to format the code better
* More test cases for utils
* Poetry for dependency management and packaging in python





