# AI Core project - Multinational retail data centralisation 

This project was part of the AI Core bootcamp in Cloud Engineering. 
The goal of the project is to collect data from AWS (S3 and RDS) and create a Postgres SQL database with the data collected. 

The cleaning of the data has been achieved using regular expressions and various pandas methods 

---- add part on schema etc...

## Installation instructions

- The repo comes with a requirements.txt file. The required packages were exported from a pipenv enviroment using 'pip freeze > requirements.txt'. 
I did not include the pipfile and pipfile.lock files to ensure that users can use their virtual environment of choice. 

- Once the environment is created and activated, the required packages can be installed with 'pip install -r requirements.txt'

- The code requires a db_creds.yaml file with the credentials to access AWS RDS, this has been added to the .gitgnore file to avoid sharing its content publicly and will cause the code to throw an error when executed without the file

- A local_cred.yaml file is also required: it contains the credentials to access a local database that was created using PgAdmin4. The user should create its own local db and file for the code to run without errors.

## Usage instructions

- Simply run main.py 
