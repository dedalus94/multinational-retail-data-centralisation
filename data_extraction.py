import pandas as pd
from database_utils import * 


class DataExtractor():

    def __init__(self) -> None:
        pass
            

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


