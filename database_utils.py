import yaml
from sqlalchemy import create_engine

class DatabaseConnector:

    def __init__(self) -> None:
        pass

    def read_db_creds(self):

        """This method reads from a yaml file that stores the credentials to access an AWS RDS"""

        with open('db_creds.yaml','r') as credentials: 

            cred_dict=yaml.safe_load(credentials)
            
            return cred_dict
    
    def init_db_engine(self):

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


            
ABC=DatabaseConnector()
print(ABC.init_db_engine())
