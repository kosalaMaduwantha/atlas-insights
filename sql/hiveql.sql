CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_transactions (
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice FLOAT,
    CustomerID INT,
    Country STRING
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_transactions/rdbms'
TBLPROPERTIES ('parquet.compression'='SNAPPY');