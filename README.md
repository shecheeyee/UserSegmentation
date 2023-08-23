# UserSegmentation
Data Engineering Project using Apache Spark and Apache Airflow

Welcome to the UserSegmentation project! This repository contains all the necessary files and instructions to help you set up an Apache Airflow environment using Docker Compose. Apache Airflow is a platform used to programmatically author, schedule, and monitor workflows.

 

## Prerequisites
Before you begin, please make sure you have the following prerequisites installed on your system:
- Docker: [Install Docker](https://docs.docker.com/get-docker/)
- Docker Compose: [Install Docker Compose](https://docs.docker.com/compose/install/)
- Apache Spark: [Install Apache Spark](https://spark.apache.org/docs/latest/#downloading)
   - Windows installation: https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/
   - Mac installation: https://sparkbyexamples.com/spark/install-apache-spark-on-mac/
   - Linux installation: https://sparkbyexamples.com/spark/spark-installation-on-linux-ubuntu/
 

## Getting Started
Follow these steps to set up the Apache Airflow environment using Docker Compose and execute SQL statements:

1. **Clone the Repository:** Start by cloning this repository to your local machine using the following command:

    ```bash
    git clone https://github.com/shecheeyee/UserSegmentation.git
    cd UserSegmentation
    ```

2. **Execute SQL Statements:** There is a text file named `sql_statements.txt` containing your SQL `CREATE TABLE` and `INSERT INTO` commands, follow these steps to execute them in your database:  
    a. Ensure you have `psql` installed and configured with the appropriate database connection details.  
    b. Run the following command to get the name of the containers, copy the one that says `postgres:13`
   
      ```bash
      docker-compose ps
      ```
       
   
    c. Run the following command to enter the postgres container
   
      ```bash
      docker exec -it postgres_container_name psql -U airflow
      ```
    d. Run the following command to execute sql commands.
   
      ```bash
      \i /var/lib/postgresql/sql_statements.txt
      ```
   
    e. The database is now created, along with the static mapping tables that is needed for the pipeline.  

 

4. **Run Docker Compose:** To start the Apache Airflow environment, execute the following command:  
    ```bash
    docker-compose up 
    ```


5. **Access the Airflow UI:** Once the services are up and running, you can access the Apache Airflow web interface by opening a web browser and navigating to [http://localhost:8080](http://localhost:8080).  
6. **Access the Airflow CMD:** Navigate into one of the airflow containers to access the airflow terminal, you can test your Operators locally in here.  
    a. Run the following command to get the name of the containers  
      ```bash
      docker-compose ps
      ```  
    b. Run the following command to connect to the container, with the bash shell.  
      ```bash
      docker exec -it name_of_container /bin/bash
      ```  
    c. Run the following command to test your Operator(s).  
      ```bash
      airflow tasks test dag_name task_name yyyy-mm-dd
      ```  


7. **Stop and Cleanup:** To stop the environment and remove the containers, execute the following command:  

    ```bash
    docker-compose down
    ```  

 

## Next Steps
The UserSegmentation pipeline will run daily, it can be found under DAGs, named `daily_ingest`. You can also manually trigger it by clicking the start button at the top right corner of the DAG's page.
 
