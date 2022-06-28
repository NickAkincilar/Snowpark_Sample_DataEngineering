# Snowpark_Sample_DataEngineering
Here is a very basic Snowflake Snowpark Python code for data engineering showing basic dataframe operations such as reading, grouping and writing data. It uses the freely available sample database built into Snowflake as the source. 

Logic is pretty simple
1. Increase the compute to 32 nodes
2. Join 2 Large tables (LINEITEMS with 6 billion rows & SUPPLIER 10 million rows) on SupplierKey,
3. Summarize the data on Supplier & PartNo to calculate sum, min & max (350 million rows)
4. Write the resulting dataframe to a physical Snowflake table
5. Decrease the compute to 1 node

Entire operation from increasing compute, reading the data, joining, summarizing & scaling down **takes about 40-45 secs** showing you the power & instant scalability & performance of Snowflake  

## Installing  Snowpark
#### Your local environment such as PyCharm, Visual Studio, Jupyter & etc.
https://docs.snowflake.com/en/developer-guide/snowpark/python/setup.html

#### To install on Databricks
Simply use the PyPI option and enter 
~~~
"snowflake-snowpark-python[pandas]"
~~~

<img src="https://raw.githubusercontent.com/NickAkincilar/Snowpark_Sample_DataEngineering/main/Screen%20Shot%202022-06-28%20at%2012.08.53%20PM.png" width="500">

### Here is the sample code

~~~python
import time
import snowflake.snowpark.functions as f
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import call_udf

CONNECTION_PARAMETERS = {
    "host": "YourSnowflakeAccount.snowflakecomputing.com",
    'account': 'YourSnowflakeAccount',
    'user': 'YourUserID',
    'role': 'SYSADMIN',
    'password': 'YourPW',
    "database": "Default_DB_Name",
    "schema": "Default_Schema_Name",
    "warehouse": "Snowflake_ComputeCluster_Name",
}


print("Connecting to Snowflake.....\n")
session = Session.builder.configs(CONNECTION_PARAMETERS).create()
print("Connected Successfully!..\n\n")

start_time = time.time()
temp_df = session.sql(
        "ALTER WAREHOUSE COMPUTE_ETL SET WAREHOUSE_SIZE = 'XXLARGE' WAIT_FOR_COMPLETION = TRUE").show()  # INCREASE TO 32 COMPUTE NODES

dfLineItems = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM")  # 6.0 Billion Rows
dfSuppliers = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER")  # 10 Million Rows

dfJoinTables = dfLineItems.join(dfSuppliers,
                                dfLineItems.col("L_SUPPKEY") == dfSuppliers.col("S_SUPPKEY"))  # JOIN TABLES

# SUMMARIZE THE DATA BY SUPPLIER, PART, SUM, MIN & MAX
dfSummary = dfJoinTables.groupBy("S_NAME", "L_PARTKEY").agg([
    f.sum("L_QUANTITY").alias("TOTAL_QTY"),
    f.min("L_QUANTITY").alias("MIN_QTY"),
    f.max("L_QUANTITY").alias("MAX_QTY"),
])

# WRITE THE DATA TO TABLE ( 365 Million Rows)
dfSummary.write.mode("overwrite").saveAsTable("TARGET_DBNAME.PUBLIC.SALES_SUMMARY")
dfSales = session.table("TARGET_DBNAME.PUBLIC.SALES_SUMMARY")
dfSales.show()
end_time = time.time()
print("--- %s seconds to Join, Summarize & Write Results to a new Table --- \n" % int(end_time - start_time))
print("--- %s Rows Written to SALES_SUMMARY table" % dfSales.count())

temp_df = session.sql("ALTER WAREHOUSE COMPUTE_ETL SET WAREHOUSE_SIZE = 'XSMALL'").show()


~~~
