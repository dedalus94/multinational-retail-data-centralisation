ALTER TABLE   orders_table  ALTER COLUMN  date_uuid         TYPE     UUID   USING date_uuid::uuid;
ALTER TABLE   orders_table  ALTER COLUMN  user_uuid         TYPE     UUID   USING user_uuid::uuid;          
ALTER TABLE   orders_table  ALTER COLUMN  card_number       TYPE     VARCHAR(255);         
ALTER TABLE   orders_table  ALTER COLUMN  store_code        TYPE     VARCHAR(255);         
ALTER TABLE   orders_table  ALTER COLUMN  product_code      TYPE     VARCHAR(255);        
ALTER TABLE   orders_table  ALTER COLUMN  product_quantity  TYPE     SMALLINT; 
ALTER TABLE   orders_table DROP COLUMN index;
            
ALTER TABLE   dim_users  ALTER COLUMN  first_name        TYPE     VARCHAR(255);
ALTER TABLE   dim_users  ALTER COLUMN  last_name         TYPE     VARCHAR(255);          
ALTER TABLE   dim_users  ALTER COLUMN  date_of_birth     TYPE     DATE;         
ALTER TABLE   dim_users  ALTER COLUMN  country_code      TYPE     VARCHAR(255);         
ALTER TABLE   dim_users  ALTER COLUMN  user_uuid         TYPE     UUID   USING user_uuid::uuid;       
ALTER TABLE   dim_users  ALTER COLUMN  join_date         TYPE     DATE;
ALTER TABLE   dim_users DROP COLUMN index;               

ALTER TABLE   dim_store_details  ALTER COLUMN  longitude        TYPE     FLOAT;
ALTER TABLE   dim_store_details  ALTER COLUMN  locality         TYPE     VARCHAR(255);          
ALTER TABLE   dim_store_details  ALTER COLUMN  store_code       TYPE     VARCHAR(255);         
ALTER TABLE   dim_store_details  ALTER COLUMN  staff_numbers    TYPE     SMALLINT;         
ALTER TABLE   dim_store_details  ALTER COLUMN  opening_date     TYPE     DATE;       
ALTER TABLE   dim_store_details  ALTER COLUMN  store_type       TYPE     VARCHAR(255);
ALTER TABLE   dim_store_details  ALTER COLUMN  store_type       DROP NOT NULL;
ALTER TABLE   dim_store_details  ALTER COLUMN  country_code     TYPE     VARCHAR(255);  
ALTER TABLE   dim_store_details  ALTER COLUMN  continent        TYPE     VARCHAR(255);
ALTER TABLE   dim_store_details DROP COLUMN index; 

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);

UPDATE dim_products
SET weight_class =  CASE
        WHEN weight_kg < 2 THEN 'Light'
        WHEN (weight_kg >= 2 and weight_kg < 40) THEN 'Mid_Sized'
        WHEN (weight_kg >= 40 and weight_kg < 140) THEN 'Heavy'
        ELSE 'Truck_Required'
    END ;

UPDATE dim_products
SET removed =  CASE
        WHEN removed ='Removed' THEN FALSE
        ELSE TRUE
    END ;
    
ALTER TABLE dim_products DROP COLUMN weight;
ALTER TABLE dim_products RENAME COLUMN weight_kg TO weight;
ALTER TABLE dim_products RENAME COLUMN removed   TO still_available;

ALTER TABLE   dim_products  ALTER COLUMN  product_price TYPE double precision USING (trim(product_price)::double precision);
ALTER TABLE   dim_products  ALTER COLUMN  product_price        TYPE     FLOAT;
ALTER TABLE   dim_products  ALTER COLUMN  weight               TYPE     FLOAT;          
ALTER TABLE   dim_products  ALTER COLUMN  "EAN"                TYPE     VARCHAR(255);         
ALTER TABLE   dim_products  ALTER COLUMN  product_code         TYPE     VARCHAR(255);        
ALTER TABLE   dim_products  ALTER COLUMN  date_added           TYPE     DATE;       
ALTER TABLE   dim_products  ALTER COLUMN  uuid                 TYPE     UUID   USING uuid::uuid;
ALTER TABLE   dim_products  ALTER COLUMN  still_available      TYPE     BOOL USING still_available::boolean;
ALTER TABLE   dim_products  ALTER COLUMN  weight_class         TYPE     VARCHAR(255);  
ALTER TABLE   dim_products DROP COLUMN index; 

ALTER TABLE   dim_date_times  ALTER COLUMN  month            TYPE     VARCHAR(255); 
ALTER TABLE   dim_date_times  ALTER COLUMN  year             TYPE     VARCHAR(255); 
ALTER TABLE   dim_date_times  ALTER COLUMN  day              TYPE     VARCHAR(255);         
ALTER TABLE   dim_date_times  ALTER COLUMN  time_period      TYPE     VARCHAR(255);         
ALTER TABLE   dim_date_times  ALTER COLUMN  date_uuid        TYPE     UUID   USING date_uuid::uuid;        
ALTER TABLE   dim_date_times DROP COLUMN index; 

ALTER TABLE   dim_card_details  ALTER COLUMN  card_number              TYPE     VARCHAR(255);
ALTER TABLE   dim_card_details  ALTER COLUMN  expiry_date              TYPE     VARCHAR(255); 
ALTER TABLE   dim_card_details  ALTER COLUMN  date_payment_confirmed   TYPE     DATE;         
ALTER TABLE   dim_card_details DROP COLUMN index; 

-- create primary and foreign keys 

ALTER TABLE dim_users ADD CONSTRAINT users_prim_key PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details ADD CONSTRAINT stores_prim_key PRIMARY KEY (store_code);
ALTER TABLE dim_products ADD CONSTRAINT products_prim_key PRIMARY KEY (product_code);
ALTER TABLE dim_date_times ADD CONSTRAINT dates_prim_key PRIMARY KEY (date_uuid);
ALTER TABLE dim_card_details ADD CONSTRAINT cards_prim_key PRIMARY KEY (card_number);

ALTER TABLE orders_table ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
ALTER TABLE orders_table ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);


