
--TASK 1
SELECT country_code as country,COUNT(country_code) as total_no_stores
FROM dim_store_details
GROUP BY country_code;


--TASK 2
SELECT locality as country,COUNT(locality) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7 ;


--TASK 3
SELECT ROUND(SUM(orders.product_quantity*products.product_price::numeric), 2) total_sales,
       dates.month
FROM orders_table AS orders 
LEFT JOIN  dim_date_times AS dates ON orders.date_uuid = dates.date_uuid
LEFT JOIN  dim_products AS products ON orders.product_code = products.product_code
GROUP BY month 
ORDER BY total_sales DESC
LIMIT 6;


--TASK 4
SELECT SUM(orders.product_quantity) product_quantity_count ,
       COUNT(orders.date_uuid) as numbers_of_sales,
       CASE WHEN store_type = 'Web Portal' THEN 'Web'
       ELSE 'Offline'
       END AS location 
FROM orders_table AS orders 
LEFT JOIN  dim_store_details AS stores ON orders.store_code = stores.store_code
LEFT JOIN  dim_products AS products ON orders.product_code = products.product_code
GROUP BY location;


--TASK 5

SELECT subquery1.store_type,
       subquery1.total_sales,
       ROUND(subquery1.total_sales * 100.0 /subquery2.tot, 2) AS "percentage_total(%)"
FROM

(SELECT store_type,
        SUM(orders.product_quantity*products.product_price::numeric) total_sales 
FROM orders_table AS orders 
LEFT JOIN  dim_store_details AS stores ON orders.store_code = stores.store_code
LEFT JOIN  dim_products AS products ON orders.product_code = products.product_code
GROUP BY store_type) subquery1

CROSS JOIN 

(SELECT SUM(orders.product_quantity*products.product_price::numeric) tot
FROM orders_table AS orders
LEFT JOIN  dim_products AS products ON orders.product_code = products.product_code) subquery2

ORDER BY "percentage_total(%)" DESC 


--TASK 6
SELECT ROUND(SUM(orders.product_quantity*products.product_price::numeric), 2) total_sales,
       dates.year,
       dates.month
FROM orders_table AS orders 
LEFT JOIN  dim_date_times AS dates ON orders.date_uuid = dates.date_uuid
LEFT JOIN  dim_products AS products ON orders.product_code = products.product_code
GROUP BY year,month 
ORDER BY total_sales DESC
LIMIT 10;


--TASK 7

SELECT SUM(staff_numbers) as total_staff_numbers,
       country_code 
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;
s

--TASK 8 

SELECT SUM(orders.product_quantity*products.product_price::numeric) total_sales,
       store_type,
       country_code

FROM orders_table AS orders 
LEFT JOIN  dim_store_details AS stores ON orders.store_code = stores.store_code
LEFT JOIN  dim_products AS products ON orders.product_code = products.product_code
WHERE country_code = 'DE'
GROUP BY country_code,store_type
ORDER BY total_sales 

--TASK 9 

SELECT subquery.year,
       AVG(subquery.diff) as actual_time_taken           
 FROM 

    (
        SELECT year,
               (LEAD (timestamp::TIME, 1) OVER (ORDER BY year,month,day,timestamp ASC ) - timestamp::TIME) as diff
        FROM dim_date_times 
    ) subquery

GROUP BY subquery.year
ORDER BY actual_time_taken  DESC







    
     
    



