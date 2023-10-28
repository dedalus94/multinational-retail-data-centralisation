import yaml

class DatabaseConnector:

    def read_db_creds():

        with open('db_creds.yaml','r') as credentials: 

            cred_dict=yaml.safe_load(credentials)
            print(cred_dict)


DatabaseConnector.read_db_creds()
