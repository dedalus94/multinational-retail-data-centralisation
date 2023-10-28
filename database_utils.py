import yaml

class DatabaseConnector:

    def read_db_creds():

        with open('db_creds.yaml','r') as credentials: 

            cred_dict=yaml.safe_load(credentials)
            
            return cred_dict
            


