-- 1. Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv

-- DONE. Stored in my local system in the below location 
/home/cloudera/assignments/sales_order_data.csv



-- 2. Store raw data into hdfs location

[cloudera@quickstart ~]$ hdfs dfs -mkdir /assignments

[cloudera@quickstart ~]$ hdfs dfs -put /home/cloudera/assignments/sales_order_data.csv /assignments



-- 3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table

[cloudera@quickstart ~]$ Hive

USE assignments;
create table sales_order_data_csv
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
row format delimited
fields terminated by ','
TBLPROPERTIES("skip.header.line.count"="1");



-- 4. Load data from hdfs path into "sales_order_csv" 

load data inpath '/assignments/sales_order_data.csv' into table sales_order_data_csv;



-- 5. Create an internal hive table which will store data in ORC format "sales_order_orc"

CREATE TABLE sales_order_data_orc
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;



-- 6. Load data from "sales_order_csv" into "sales_order_orc"

FROM sales_order_data_csv INSERT OVERWRITE TABLE sales_order_data_orc SELECT *;



-- Perform below menioned queries on "sales_order_orc" table :

-- a. Calculate total sales per year

SELECT year_id, SUM(sales) as total_sales
FROM sales_order_data_orc
GROUP BY year_id;


-- b. Find a product for which maximum orders were placed

SELECT productcode, COUNT(ordernumber) AS total_orders
FROM sales_order_data_orc
GROUP BY productcode
ORDER BY total_orders DESC
LIMIT 1;
    

-- c. Calculate the total sales for each quarter

SELECT qtr_id,
SUM(sales) AS total_sales
FROM sales_order_data_orc
GROUP BY qtr_id;
    


-- d. In which quarter sales was minimum

SELECT qtr_id,
SUM(sales) AS total_sales
FROM sales_order_data_orc
GROUP BY qtr_id
ORDER BY total_sales
LIMIT 1;
    

    
-- e. In which country sales was maximum and in which country sales was minimum

-- Country with Maximum Sales -

SELECT country,
SUM(sales) AS total_sales
FROM sales_order_data_orc
GROUP BY country
ORDER BY total_sales DESC
LIMIT 1;


-- Country with Maximum Sales -

SELECT country,
SUM(sales) AS total_sales
FROM sales_order_data_orc
GROUP BY country
ORDER BY total_sales
LIMIT 1;
    


-- f. Calculate quarterly sales for each city

SELECT city,
qtr_id,
SUM(sales) AS quarterly_sales
FROM sales_order_data_orc
GROUP BY city, qtr_id;
 


-- h. Find a month for each year in which maximum number of quantities were sold

SELECT year_id,month_id,total_quantity
FROM
(SELECT year_id, month_id, SUM(quantityordered) as total_quantity,
row_number () over
(
PARTITION BY year_id
ORDER BY total_quantity DESC
) AS rn
FROM sales_order_data_orc
GROUP BY year_id, month_id
) AS main
WHERE rn =1;