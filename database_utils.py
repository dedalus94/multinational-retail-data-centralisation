import yaml
from sqlalchemy import create_engine,inspect
from sqlalchemy import text

import pandas as pd 

class DatabaseConnector:

    def __init__(self) -> None:
        pass

    def read_db_creds(self):

        """This method reads from a yaml file that stores the credentials to access an AWS RDS"""

        with open('db_creds.yaml','r') as credentials: 

            cred_dict=yaml.safe_load(credentials)
            
            return cred_dict
    
    def init_db_engine(self):

        """ This method creates and returns an SQL alchemy engine created using the credentials read from the other method"""

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = self.read_db_creds()['RDS_HOST'] 
        USER = self.read_db_creds()['RDS_USER'] 
        PASSWORD = self.read_db_creds()['RDS_PASSWORD'] 
        PORT = self.read_db_creds()['RDS_PORT'] 
        DATABASE = self.read_db_creds()['RDS_DATABASE'] 


        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine = engine.connect()

        return engine
    
    def list_db_tables(self):

        insp = inspect(self.init_db_engine())
        print (insp.get_table_names())

    def upload_to_db(self, table_name,df):

        """This function connects to the locally initialised DB and uploads data in a table."""

        with open('local_cred.yaml','r') as local_credentials:

            local_cred_dict=yaml.safe_load(local_credentials)

            DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            ENDPOINT = local_cred_dict['HOST'] 
            USER = local_cred_dict['USER'] 
            PASSWORD = local_cred_dict['PASSWORD'] 
            PORT = local_cred_dict['PORT'] 
            DATABASE = local_cred_dict['DATABASE'] 

            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
            engine = engine.connect()
            
            df.to_sql(name=table_name,
                    con=engine,
                    if_exists='replace',
                    )






        




