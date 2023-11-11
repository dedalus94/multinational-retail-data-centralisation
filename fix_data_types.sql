
/**

ALTER TABLE   orders_table  ALTER COLUMN  date_uuid         TYPE     UUID   USING date_uuid::uuid;
ALTER TABLE   orders_table  ALTER COLUMN  user_uuid         TYPE     UUID   USING user_uuid::uuid;          
ALTER TABLE   orders_table  ALTER COLUMN  card_number       TYPE     VARCHAR(255);         
ALTER TABLE   orders_table  ALTER COLUMN  store_code        TYPE     VARCHAR(255);         
ALTER TABLE   orders_table  ALTER COLUMN  product_code      TYPE     VARCHAR(255);        
ALTER TABLE   orders_table  ALTER COLUMN  product_quantity  TYPE     SMALLINT 
            
**/
/**
ALTER TABLE   dim_users  ALTER COLUMN  first_name        TYPE     VARCHAR(255);
ALTER TABLE   dim_users  ALTER COLUMN  last_name         TYPE     VARCHAR(255);          
ALTER TABLE   dim_users  ALTER COLUMN  date_of_birth     TYPE     DATE;         
ALTER TABLE   dim_users  ALTER COLUMN  country_code      TYPE     VARCHAR(255);         
ALTER TABLE   dim_users  ALTER COLUMN  user_uuid         TYPE     UUID   USING user_uuid::uuid;       
ALTER TABLE   dim_users  ALTER COLUMN  join_date         TYPE     DATE               
**/







INSERT INTO dim_store_details
        (latitude)
(SELECT 
    COALESCE(lat,latitude)
    from dim_store_details);

ALTER TABLE   dim_store_details  ALTER COLUMN  longitude        TYPE     FLOAT;
ALTER TABLE   dim_store_details  ALTER COLUMN  locality         TYPE     VARCHAR(255);          
ALTER TABLE   dim_store_details  ALTER COLUMN  store_code       TYPE     VARCHAR(255);         
ALTER TABLE   dim_store_details  ALTER COLUMN  staff_numbers    TYPE     SMALLINT;         
ALTER TABLE   dim_store_details  ALTER COLUMN  opening_date     TYPE     DATE;       
ALTER TABLE   dim_store_details  ALTER COLUMN  store_type       TYPE     VARCHAR(255);
ALTER TABLE   dim_store_details  ALTER COLUMN  store_type       DROP NOT NULL;
ALTER TABLE   dim_store_details  ALTER COLUMN  country_code     TYPE     VARCHAR(255);  
ALTER TABLE   dim_store_details  ALTER COLUMN  continent        TYPE     VARCHAR(255)  






