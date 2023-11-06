import pandas as pd
from database_utils import * 
import requests 
from tqdm import tqdm
import boto3 


class DataExtractor():

    '''
    A class used to extract data from different sources

    
    Parameters:
    ----------
    None
    
    Attributes:
    ----------
    endpoint (str): AWS API endpoint 
    header (str): header to authenticate when using get requests 


    Methods:
    -------


    read_rds_table(connector,table_name)
        returns a pandas DataFrame from a RDS table

    retrieve_pdf_data(link)
        returns a pandas DataFrame from a pdf stored in AWS

    list_number_of_stores()
        lists the number of stored available through the API (API endpoint stored is a class parameter)

    retrieve_stores_data()
        retrieves the stores data by looping through all the stores and returns a DataFrame (API endpoint stored is a class parameter)

    extract_from_s3(s3_address, desired_file_name=None)
        downloads a csv from S3 and returns a pandas DataFrame

    extract_json(url)
        runs a get request to load a json file into a pandas DataFrame and returns the DataFrame 

    '''

    def __init__(self) -> None:

        #initialising attributes that wll be used in the API call methods 
        self.endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/'
        self.header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}  

    def read_rds_table(self,connector,table_name):

        """
        reads a table 

        Args:
            -connector (SQL alchemy engine): an engine can be generated using DatabaseConnector.init_db_engine
            -table_name (str): desired name for the dataframe to upload as a table
            -df (pandas dataframe): dataframe to upload 

        Returns:
            Pandas DataFrame 
        
        """

        df = pd.read_sql_table(table_name, connector.init_db_engine())

        return df
    
    def retrieve_pdf_data(self, link):

        """
        reads a pdf document stored at the link (to S3) provided as an argument.  

        Args:
            -link (str): url to the PDF document stored in S3

        Returns:
            Pandas DataFrame 
        
        """

        return (pd.concat(tabula.read_pdf(link, stream=True, pages="all",output_format='dataframe')))
    

    def list_number_of_stores(self):

        """ 
        Lists the number of stored available through the API with endpoint stored in this class attributes.
        The path used in this method has the sole purpose of listing the number of stored that can be queries 

        Args:
            None

        Returns:
            Str 

        """

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

        """ 
        Loops through all the stores available through the API with endpoint stored in this class attributes.
        The path used in this method can retrieve the data for a single stored the number of which is specified as a parameter. 
        This method appends data from each store to a pandas DataFrame

        Args:
            None

        Returns:
            Pandas DataFrame  

        """

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

        """ 
        Downloads, reads and returns (as a pandas DataFrame) a CSV stored in S3. The s3_address argument should be in the format of an S3 URI: s3://bucket-name/path/file
        The string is split so that the bucket name and the path to the file are extracted, these are required by boto3 to download the file.
        The desired_file_name is None by default and in that case is extracted from path,
        otherwise is specified by the user. 

        Args:
            - s3_address (str) : must be an S3 AWS URI
            - desired_file_name (str) : desired name for the file that is saved locally  

        Returns:
            Pandas DataFrame 

        """


        s3_address = s3_address[5:].split('/')
        bucket_name = s3_address[0]

        #this type of processing is useful if the file is in a folder within the bucker
        path = s3_address[1:]
        path = '/'.join(path)

        if desired_file_name == None: 
            desired_file_name = s3_address[-1]
        else: 
            pass 

        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.download_file(bucket_name , path , desired_file_name)

        return pd.read_csv(desired_file_name,index_col=0)
    
    def extract_json(self,url):

        """
        runs a get request to read a json document from the link (url) provided as an argument.  

        Args:
            -link (str): url json

        Returns:
            Pandas DataFrame 
        
        """
        
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











