CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_transactions_fs_ecommerce_transactions (
    Transaction_ID INT,
    User_Name STRING,
    Age INT,
    Country STRING,
    Product_Category STRING,
    Purchase_Amount DECIMAL,
    Payment_Method STRING,
    Transaction_Date DATE
) 
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_transactions/fs'
TBLPROPERTIES ('parquet.compression'='SNAPPY'); 

