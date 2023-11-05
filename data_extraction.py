import pandas as pd
from database_utils import * 
import requests 
from tqdm import tqdm
import boto3 


class DataExtractor():



    def __init__(self) -> None:

        #initialising attributes that wll be used in the API call methods 
        self.endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/'
        self.header= {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        
        
            

    def read_rds_table(self,connector,table_name):

        """
        reads a table 

        --- parameters --- 
        - instance of connector 
        - table name 
        
        
        """

        df = pd.read_sql_table(table_name, connector.init_db_engine())

        return df
    
    def retrieve_pdf_data(self, link):

        return (pd.concat(tabula.read_pdf(link, stream=True, pages="all",output_format='dataframe')))
    

    def list_number_of_stores(self):

        path = 'prod/number_stores'
        url = self.endpoint + path 
        
        response = requests.get(url, headers=self.header)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
        
            print('request successful')
            data = response.json()
            number_stores = data['number_stores']
            print(f"Number of stores: {number_stores}")

        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

        return number_stores

    
    def retrieve_stores_data(self):

        path = 'prod/store_details/'
        store_data=[]

        ## NOTE -- there are 451 stores with last index 450:
        for store_number in tqdm(range(self.list_number_of_stores())):

            url = self.endpoint + path + str(store_number)
            response = requests.get(url, headers=self.header)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
            
                data = response.json()
                store_data.append(pd.DataFrame(data=data,index=[data['index']]))
            
            else: 
                print(f"Request failed for store number {store_number} with status code: {response.status_code}")
                print(f"Response Text: {response.text}")               
        
        return pd.concat(store_data)
    
    def extract_from_s3(self,s3_address, desired_file_name=None):

        s3_address=s3_address[5:].split('/')
        bucket_name=s3_address[0]

        #this type of processing is useful if the file is in a folder within the bucker
        path= s3_address[1:]
        path='/'.join(path)

        if desired_file_name==None: 
            desired_file_name=s3_address[-1]
        else: 
            pass 

        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.download_file(bucket_name , path , desired_file_name)

        return pd.read_csv(desired_file_name,index_col=0)
    
    def extract_json(self,url):
        
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
        
            print('request successful')
            data = response.json()
            data = pd.DataFrame(data)

        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

        return data











