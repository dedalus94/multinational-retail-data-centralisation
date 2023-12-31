# AI Core project - Multinational retail data centralisation 

This project is part of the AI Core bootcamp in Cloud Engineering. 
The goal of the project is to collect data from AWS (S3 and RDS) and create a Postgres SQL database with the data collected. 

The cleaning of the data has been achieved using regular expressions and various pandas methods.

The cleaned data has been stored in a local SQL server Database. Datatypes were fixed using an SQL script. Minor formatting, column deletions and the creation of a star-based schema has also been done by the same script.

Python concepts used in this project:

-  Classes & methods
-  List comprehension
-  Lambda functions with Pandas apply
-  Regular expressions
-  Docstrings and in-line documentation
-  Error handling (e.g. response codes from requests)

Python Libraries: 

- boto3 to extract from AWS S3
- SQL Alchemy to extract from AWS RDS and to upload into the local Postgres DB 
- Tabula 
- requests 

SQL concepts used in this project:

- ALTER TABLE ALTER COLUMN to change data types
- creation of a new column using CASE WHEN on a column in the same table
- Primary and Foreign keys
- Joins, cross joins, CASE WHEN to extract and format data

## Installation instructions

- The repo comes with a requirements.txt file. The required packages were exported from a pipenv enviroment using 'pip freeze > requirements.txt'. 
I did not include the pipfile and pipfile.lock files since users may want to use their virtual environment of choice. 

- Once the environment is created and activated, the required packages can be installed with 'pip install -r requirements.txt'

- The code requires a db_creds.yaml file with the credentials to access AWS RDS, this has been added to the .gitgnore file to avoid sharing its content publicly and will cause the code to throw an error when executed without the file

- A local_cred.yaml file is also required: it contains the credentials to access a local database that was created using PgAdmin4. The user should create its own local db and file for the code to run without errors.

- To access AWS resources, the user should install AWS CLI (command line interface) and configure it by using an access key and a secret access key pair that can be generated for an AWS user via IAM. 

## Usage instructions

- Simply run main.py and then run the SQL code. The database will be created if the steps in the installation sections were previously followed. 

## File structure of the project

Everything is in the same folder.
Three classes are used to handle the ETL process. The classes are defined in the following files and imported in the main.py script:

- **data_extraction.py**
  - contains methods to download data from different sources 
- **database_utils.py**
  - Contains all methods to connect to RDS and the local DB 
- **data_cleaning.py**
  - Contains methods unique to each data source that clean each table as well as methods used multiple times (e.g. to format dates)

Two SQL files are also in the folder:
**fix_data_types.sql** that handles data type, minor data manipulation as well as creating the star-based schema.
And **milestone_4.sql** file that contains examples of SQL queries to get insights from the data and it also serves the purpose of validating the data, given that the result of the queries has been compared to the assignment.


