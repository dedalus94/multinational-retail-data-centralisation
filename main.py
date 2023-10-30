from database_utils import * 
from data_extraction import * 
from data_cleaning import * 





if __name__ == "__main__":

    # Instantiates classes: 

    db= DatabaseConnector()
    db_extractor= DataExtractor()
    db_cleaner=DataCleaning()

    # using methods: 

    #db.list_db_tables() # list all RDS tables 

    #users_df=db_extractor.read_rds_table(db,'legacy_users') #extracts RDS users table

    #print(users_df.shape[0]) #pre-processing number of rows 
    #users_df=db_cleaner.clean_user_data(users_df) # clean RDS users table 
    #print(users_df.shape[0]) #post-processing number of rows 

    #db.upload_to_db('local_cred.yaml','dim_users',users_df) #stores users data locally 
    
    link= 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf' #Document stored in S3
    pdf_df=db_extractor.retrieve_pdf_data(link) #extracts card data from pdf 

    pdf_df=db_cleaner.clean_card_data(pdf_df)

    db.upload_to_db('local_cred.yaml','dim_card_details',pdf_df)