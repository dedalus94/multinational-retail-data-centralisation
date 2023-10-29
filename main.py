from database_utils import * 
from data_extraction import * 
from data_cleaning import * 





if __name__ == "__main__":
    
    db= DatabaseConnector()
    db_extractor= DataExtractor()
    db_cleaner=DataCleaning()
    db.list_db_tables()
    users_df=db_extractor.read_rds_table(db,'legacy_users')
    print(users_df.shape[0])
    users_df=db_cleaner.clean_user_data(users_df)
    print(users_df.shape[0])


