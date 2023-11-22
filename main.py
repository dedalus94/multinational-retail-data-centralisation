from database_utils import * 
from data_extraction import * 
from data_cleaning import * 


if __name__ == "__main__":

    # Instantiates classes: 

    db = DatabaseConnector()
    db_extractor = DataExtractor()
    db_cleaner = DataCleaning()

    # using methods - check dockstring to know what each method does in detail: 

 
    db.list_db_tables() # list all RDS tables 

    users_df = db_extractor.read_rds_table(db,'legacy_users') #extracts RDS users table

    print(users_df.shape[0]) #pre-processing number of rows 
    users_df = db_cleaner.clean_user_data(users_df) # clean RDS users table 
    print(users_df.shape[0]) #post-processing number of rows 

    db.upload_to_db('local_cred.yaml','dim_users',users_df) #stores users data locally 
   
    link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf' #Document stored in S3
    pdf_df = db_extractor.retrieve_pdf_data(link) #extracts card data from pdf 
    pdf_df = db_cleaner.clean_card_data(pdf_df)

    db.upload_to_db('local_cred.yaml','dim_card_details',pdf_df)

   

    #gets stores data from API 
    number_of_stores = db_extractor.list_number_of_stores()
    store_df = db_extractor.retrieve_stores_data()
    store_df = db_cleaner.called_clean_store_data(store_df)
    db.upload_to_db('local_cred.yaml','dim_store_details',store_df)
    
  

    #gets products data from a csv stored in S3, cleans and process the weight column, uploads to the postgres db:
    s3_file = db_extractor.extract_from_s3('s3://data-handling-public/products.csv','s3_products.csv')
    s3_file = db_cleaner.clean_products_data(s3_file)
    s3_file = db_cleaner.convert_product_weights(s3_file)
    
    db.upload_to_db('local_cred.yaml','dim_products',s3_file)
    
    
    db.list_db_tables()

    #extracts RDS orders table, cleans the data, uploads to the postgres db:
    orders_df = db_extractor.read_rds_table(db,'orders_table')
    orders_df = db_cleaner.clean_orders_data(orders_df)
    db.upload_to_db('local_cred.yaml','orders_table',orders_df)


    #converts the sales json into pandas dataframe, uploads to the postgres db:
    sales_df = db_extractor.extract_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    sales_df = db_cleaner.clean_sales_data(sales_df)
    db.upload_to_db('local_cred.yaml','dim_date_times',sales_df)
 