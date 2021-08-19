# Pyspark Weather ETL (WIP)

***Description***

Current Weather ETL Data Engineering Project. Script extracts the current weather of 2 different countries, verifies that the data doesn't have null values nor any primary key is violated and then uploads the data into two different Postgresql tables using SQLAlchemy. 

***Essentials***
- Extra libraries that must be imported: sys, json, datetime. 

***ETL Execution***
- Install all the necessary libraries from the Pipfile.
- Got to Rapid API (https://rapidapi.com/hub), create an account and use the url that is inside the extraction module to acces the weather API. 
- Create SQL Database/Table (Optional)
- Create a bash file. This file is were you'll write down the path to Spark, Python and your script. If this isn't created you'll get the "ModuleNotFoundError" for each module you import inside your script. (Think of this as the ETL's own ~/.bash_profile)
- Create a new crontab or use the existing one if you want the job to run hourly every day.

***Extras***
- To verify that your scheduled job is working you can change the crontab to "* * * * *".
- Airflow folder included. This has the DAG file + Modules for Apache Airflow Scheduler. The Modules folder includes the Original weather module divided into two Weather_A and Weather_B. 