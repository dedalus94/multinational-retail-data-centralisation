from database_utils import * 
from data_extraction import * 


db= DatabaseConnector()
db_extractor= DataExtractor()
db.list_db_tables()
print(db_extractor.read_rds_table(db,'legacy_users'))
