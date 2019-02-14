
# 1 : Instructions

# Connect to the MySQL database on the itversity labs using sqoop and import all of the data from the orders table into HDFS

# Data Description

# A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 68883 rows of orders data

# MySQL database information:

# Installation on the node ms.itversity.com
# Database name is retail_db
# Username: retail_user
# Password: itversity
# Table name orders
# Output Requirements

# Place the customer files in the HDFS directory
# /user/`whoami`/problem1/solution/
# Replace `whoami` with your OS user name
# Use a text format with comma as the columnar delimiter
# Load every order record completely
# End of Problem

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/codersham/problem1/solution \
--as-textfile \
--fields-terminated-by ','

#2 :Instructions

# Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname

# Data Description

# Data is available in local file system /data/retail_db

# retail_db information:

# Source directories: /data/retail_db/orders and /data/retail_db/customers
# Source delimiter: comma(",")
# Source Columns - orders - order_id, order_date, order_customer_id, order_status
# Source Columns - customers - customer_id, customer_fname, customer_lname and many more
# Output Requirements

# Target Columns: customer_lname, customer_fname
# Number of Files: 1
# Place the output file in the HDFS directory
# /user/`whoami`/problem2/solution/
# Replace `whoami` with your OS user name
# File format should be text
# delimiter is (",")
# Compression: Uncompressed
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

orders_rdd = sc.textFile('/public/retail_db/orders'). \
map(lambda rec: Row(
	order_id = int(rec.split(',')[0]), 
	order_customer_id =int(rec.split(',')[2])
	))

orders_DF = sqlContext.createDataFrame(orders_rdd)

orders_DF.registerTempTable('orders')

cusotmers_rdd = sc.textFile('/public/retail_db/customers'). \
map(lambda rec: Row(
	customer_id = int(rec.split(',')[0]), 
	customer_fname = rec.split(',')[1], 
	customer_lname = rec.split(',')[2]))

customers_DF = sqlContext.createDataFrame(cusotmers_rdd)

customers_DF.registerTempTable('customers')

result = sqlContext.sql("select distinct customer_lname, customer_fname from (select c.customer_lname, c.customer_fname, o.order_id from customers c left outer join orders o on c.customer_id = o.order_customer_id)q where q.order_id is NULL")

sqlContext.setConf('spark.sql.shuffle.partitions','1') 

result. \
selectExpr("concat(customer_lname,',',customer_fname)"). \
write. \
mode('overwrite'). \
text('/user/codersham/problem2/solution')

# 3: Instructions

# Get top 3 crime types based on number of incidents in RESIDENCE area using "Location Description"

# Data Description

# Data is available in HDFS under /public/crime/csv

# crime data information:

# Structure of data: (ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrst, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate, Y Coordinate, Year, Updated on, Latitude, Longitude, Location)
# File format - text file
# Delimiter - "," (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
# Output Requirements

# Output Fields: crime_type, incident_count
# Output File Format: JSON
# Delimiter: N/A
# Compression: No
# Place the output file in the HDFS directory
# /user/`whoami`/problem3/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

crime_data_with_header = sc.textFile('/public/crime/csv')

crime_data_only_header = sc.parallelize([crime_data_with_header.first()])

from pyspark.sql import Row

crime_data_rdd = crime_data_with_header.subtract(crime_data_only_header). \
filter(lambda rec: rec.split(',')[7] == 'RESIDENCE'). \
map(lambda rec: Row(
	id = int(rec.split(',')[0]),
	crime_type = rec.split(',')[5]
	))

crime_data_DF = sqlContext.createDataFrame(crime_data_rdd)

crime_data_DF.registerTempTable('crime_data')

result = sqlContext.sql("select crime_type, incident_count from (select crime_type, incident_count, dense_rank() over(order by incident_count desc) rnk from(select crime_type, count(id) incident_count from crime_data group by crime_type)q)m where m.rnk < 4")

result.write.json('/user/codersham/problem3/solution/')

#5 :Instructions

# Get word count for the input data using space as delimiter (for each word, we need to get how many times it is repeated in the entire input data set)

# Data Description

# Data is available in HDFS /public/randomtextwriter

# word count data information:

# Number of executors should be 10
# executor memory should be 3 GB
# Executor cores should be 20 in total (2 per executor)
# Number of output files should be 8
# Avro dependency details: groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
# Output Requirements

# Output File format: Avro
# Output fields: word, count
# Compression: Uncompressed
# Place the customer files in the HDFS directory
# /user/`whoami`/problem5/solution/
# Replace `whoami` with your OS user name
# End of Problem

pyspark \
--master yarn \
--conf spark.ui.port=12340 \
--num-executors 10 \
--executor-memory 3g \
--executor-cores 2 \
--packages com.databricks:spark-avro_2.10:2.0.1

sc.setLogLevel('ERROR')

word_rdd = sc.textFile('/public/randomtextwriter'). \
flatMap(lambda rec: rec.split(' ')). \
map(lambda rec:(rec,1))

word_count = word_rdd.reduceByKey(lambda x,y: x+y)

from pyspark.sql import Row

word = word_count. \
map(lambda rec: Row(
	word = rec[0],
	count = int(rec[1])
	))

word_DF = sqlContext.createDataFrame(word)

sqlContext.setConf('spark.sql.shuffle.partitions','8') # Not working to be re-worked

word_DF. \
write. \
format('com.databricks.spark.avro'). \
save('/user/codersham/problem5/solution/')

#6 : Instructions

# Get total number of orders for each customer where the cutomer_state = 'TX'

# Data Description

# retail_db data is available in HDFS at /public/retail_db

# retail_db data information:

# Source directories: /public/retail_db/orders and /public/retail_db/customers
# Source Columns - orders - order_id, order_date, order_customer_id, order_status
# Source Columns - customers - customer_id, customer_fname, customer_lname, customer_state (8th column) and many more
# delimiter: (",")
# Output Requirements

# Output Fields: customer_fname, customer_lname, order_count
# File Format: text
# Delimiter: Tab character (\t)
# Place the result file in the HDFS directory
# /user/`whoami`/problem6/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

orders_rdd = sc.textFile('/public/retail_db/orders'). \
map(lambda rec: Row(
	order_id = int(rec.split(',')[0]),
	order_customer_id = int(rec.split(',')[2])
	))

orders_DF = sqlContext.createDataFrame(orders_rdd)

orders_DF.registerTempTable('orders')

customers_rdd = sc.textFile('/public/retail_db/customers'). \
map(lambda rec: Row(
	customer_id = int(rec.split(',')[0]),
	customer_fname = rec.split(',')[1],
	customer_lname = rec.split(',')[2],
	customer_state = rec.split(',')[7]
	))

customers_DF = sqlContext.createDataFrame(customers_rdd)

customers_DF.registerTempTable('customers')

result = sqlContext.sql("select c.customer_fname, c.customer_lname, count(o.order_id) order_count from customers c join orders o on c.customer_id = o.order_customer_id where c.customer_state = 'TX' group by c.customer_fname, c.customer_lname")

sqlContext.setConf('spark.sql.shuffle.partitions','1')

result.selectExpr("concat(customer_fname,'\t',customer_lname,'\t',order_count)"). \
write. \
mode('overwrite'). \
text('/user/codersham/problem6/solution/')

#7 : Instructions

# List the names of the Top 5 products by revenue ordered on '2013-07-26'. Revenue is considered only for COMPLETE and CLOSED orders.

# Data Description

# retail_db data is available in HDFS at /public/retail_db

# retail_db data information:

# Source directories: 
# /public/retail_db/orders 
# /public/retail_db/order_items 
# /public/retail_db/products
# Source delimiter: comma(",")
# Source Columns - orders - order_id, order_date, order_customer_id, order_status
# Source Columns - order_items - order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price
# Source Columns - products - product_id, product_category_id, product_name, product_description, product_price, product_image
# Output Requirements

# Target Columns: order_date, order_revenue, product_name, product_category_id
# Data has to be sorted in descending order by order_revenue
# File Format: text
# Delimiter: colon (:)
# Place the output file in the HDFS directory
# /user/`whoami`/problem7/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

orders_rdd = sc.textFile('/public/retail_db/orders'). \
map(lambda rec: Row(
	order_id = int(rec.split(',')[0]), 
	order_date = rec.split(',')[1],
	order_status = rec.split(',')[3]))

orders_DF = sqlContext.createDataFrame(orders_rdd)

orders_DF.registerTempTable('orders')

orderItems_rdd = sc.textFile('/public/retail_db/order_items'). \
map(lambda rec: Row(
	order_item_order_id = int(rec.split(',')[1]), 
	order_item_product_id = int(rec.split(',')[2]), 
	order_item_subtotal = float(rec.split(',')[4])
	))

orderItems_DF = sqlContext.createDataFrame(orderItems_rdd)

orderItems_DF.registerTempTable('order_items')

products_rdd = sc.textFile('/public/retail_db/products'). \
map(lambda rec: Row(
	product_id = int(rec.split(',')[0]),
	product_category_id = int(rec.split(',')[1]),
	product_name = rec.split(',')[2]
	))

products_DF = sqlContext.createDataFrame(products_rdd)

products_DF.registerTempTable('products')

result =  sqlContext.sql("select order_date, order_revenue, product_name, product_category_id from (select order_date, order_revenue, product_name, product_category_id, dense_rank() over(order by order_revenue desc) rnk from(select o.order_date, round(sum(oi.order_item_subtotal),2) order_revenue, p.product_name, p.product_category_id from products p join order_items oi on p.product_id = oi.order_item_product_id join orders o on oi.order_item_order_id = o.order_id where to_date(o.order_date) = '2013-07-26' group by p.product_name, p.product_category_id, o.order_date)q)m where m.rnk < 6")

sqlContext.setConf('spark.sql.shuffle.partitions','1')

result. \
selectExpr("concat(order_date,':',order_revenue,':',product_name,':',product_category_id)"). \
write. \
text('/user/codersham/problem7/solution/')

#8 : Instructions

# List the order Items where the order_status = 'PENDING PAYMENT' order by order_id

# Data Description

# Data is available in HDFS location

# retail_db data information:

# Source directories: /public/retail_db/orders
# Source delimiter: comma(",")
# Source Columns - orders - order_id, order_date, order_customer_id, order_status
# Output Requirements

# Target columns: order_id, order_date, order_customer_id, order_status
# File Format: orc
# Place the output files in the HDFS directory
# /user/`whoami`/problem8/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

orders_rdd = sc.textFile('/public/retail_db/orders'). \
map(lambda rec: Row(
	order_id = int(rec.split(',')[0]), 
	order_date = rec.split(',')[1], 
	order_customer_id = int(rec.split(',')[2]), 
	order_status = rec.split(',')[3])
)

orders_DF = sqlContext.createDataFrame(orders_rdd)

orders_DF.registerTempTable('orders')

result = sqlContext.sql("select * from orders where order_status = 'PENDING PAYMENT' order by order_id")

result. \
write. \
orc('/user/codersham/problem8/solution/')

#9 : Instructions

# Remove header from h1b data

# Data Description

# h1b data with ascii null "\0" as delimiter is available in HDFS

# h1b data information:

# HDFS location: /public/h1b/h1b_data
# First record is the header for the data
# Output Requirements

# Remove the header from the data and save rest of the data as is
# Data should be compressed using snappy algorithm
# Place the H1B data in the HDFS directory
# /user/`whoami`/problem9/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

h1b_data_with_header = sc.textFile('/public/h1b/h1b_data')

h1b_data_only_header = sc.parallelize([h1b_data_with_header.first()])

h1b_data = h1b_data_with_header.subtract(h1b_data_only_header)

h1b_data.saveAsTextFile('/user/codersham/problem9/solution/','org.apache.hadoop.io.compress.SnappyCodec')

#10 : Instructions

# Get number of LCAs filed for each year

# Data Description

# h1b data with ascii null "\0" as delimiter is available in HDFS

# h1b data information:

# HDFS Location: /public/h1b/h1b_data
# Ignore first record which is header of the data
# YEAR is 8th field in the data
# There are some LCAs for which YEAR is NA, ignore those records
# Output Requirements

# File Format: text
# Output Fields: YEAR, NUMBER_OF_LCAS
# Delimiter: Ascii null "\0"
# Place the output files in the HDFS directory
# /user/`whoami`/problem10/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

h1b_data_with_header = sc.textFile('/public/h1b/h1b_data'). \
filter(lambda rec: rec.split('\0')[7] != 'NA')

h1b_data_only_header = sc.parallelize([h1b_data_with_header.first()])

h1b_data = h1b_data_with_header.subtract(h1b_data_only_header)

h1b_data_map = h1b_data.map(lambda rec: (rec.split('\0')[7],1))

h1b_data_count = h1b_data_map.reduceByKey(lambda x,y:x+y)

result = h1b_data_count.map(lambda rec: rec[0]+'\0'+str(rec[1]))

result. \
coalesce(1). \
saveAsTextFile('/user/codersham/problem10/solution/')


#11: Instructions

# Get number of LCAs by status for the year 2016

# Data Description

# h1b data with ascii null "\0" as delimiter is available in HDFS

# h1b data information:

# HDFS Location: /public/h1b/h1b_data
# Ignore first record which is header of the data
# YEAR is 8th field in the data
# STATUS is 2nd field in the data
# There are some LCAs for which YEAR is NA, ignore those records
# Output Requirements

# File Format: json
# Output Field Names: year, status, count
# Place the output files in the HDFS directory
# /user/`whoami`/problem11/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

h1b_data_with_header = sc.textFile('/public/h1b/h1b_data'). \
filter(lambda rec: rec.split('\0')[7] != 'NA')

h1b_data_only_header = sc.parallelize([h1b_data_with_header.first()])

from pyspark.sql import Row

h1b_data = h1b_data_with_header.subtract(h1b_data_only_header). \
map(lambda rec: Row(
	year = rec.split('\0')[7], 
	status = rec.split('\0')[1]
	))

h1b_data_DF = sqlContext.createDataFrame(h1b_data)

h1b_data_DF.registerTempTable('h1b_data')

result =  sqlContext.sql("select year, status, count(*) count from h1b_data where year = '2016' group by year, status")

result. \
write. \
json('/user/codersham/problem11/solution', mode = 'overwrite')


#12 : Instructions

# Get top 5 employers for year 2016 where the status is WITHDRAWN or CERTIFIED-WITHDRAWN or DENIED

# Data Description

# h1b data with ascii null "\0" as delimiter is available in HDFS

# h1b data information:

# HDFS Location: /public/h1b/h1b_data
# Ignore first record which is header of the data
# YEAR is 7th field in the data
# STATUS is 2nd field in the data
# EMPLOYER is 3rd field in the data
# There are some LCAs for which YEAR is NA, ignore those records
# Output Requirements

# File Format: parquet
# Output Fields: employer_name, lca_count
# Data needs to be in descending order by count
# Place the output files in the HDFS directory
# /user/`whoami`/problem12/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

h1b_data_with_header = sc.textFile('/public/h1b/h1b_data'). \
filter(lambda rec: rec.split('\0')[7] != 'NA')

h1b_data_only_header = sc.parallelize([h1b_data_with_header.first()])

from pyspark.sql import Row

h1b_data = h1b_data_with_header.subtract(h1b_data_only_header). \
map(lambda rec: Row(
	year = rec.split('\0')[7], 
	employer_name = rec.split('\0')[2],
	status = rec.split('\0')[1]
	))

h1b_data_DF = sqlContext.createDataFrame(h1b_data)

h1b_data_DF.registerTempTable('h1b_data')

result = sqlContext.sql("select employer_name,lca_count from(select employer_name,lca_count,dense_rank() over(order by lca_count desc) rnk from (select employer_name, count(*) lca_count from h1b_data where year = '2016' and status in ('WITHDRAWN','CERTIFIED-WITHDRAWN','DENIED') group by employer_name)q)m where m.rnk < 6")

sqlContext.setConf('spark.sql.shuffle.partitions','1')

result.write.parquet('/user/codersham/problem12/solution/')


#13 : Instructions

# Copy all h1b data from HDFS to Hive table excluding those where year is NA or prevailing_wage is NA

# Data Description

# h1b data with ascii null "\0" as delimiter is available in HDFS

# h1b data information:

# HDFS Location: /public/h1b/h1b_data_noheader
# Fields: 
# ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
# Ignore data where PREVAILING_WAGE is NA or YEAR is NA
# PREVAILING_WAGE is 7th field
# YEAR is 8th field
# Number of records matching criteria: 3002373
# Output Requirements

# Save it in Hive Database
# Create Database: CREATE DATABASE IF NOT EXISTS `whoami`
# Switch Database: USE `whoami`
# Save data to hive table h1b_data
# Create table command:

# CREATE TABLE h1b_data (
#   ID                 INT,
#   CASE_STATUS        STRING,
#   EMPLOYER_NAME      STRING,
#   SOC_NAME           STRING,
#   JOB_TITLE          STRING,
#   FULL_TIME_POSITION STRING,
#   PREVAILING_WAGE    DOUBLE,
#   YEAR               INT,
#   WORKSITE           STRING,
#   LONGITUDE          STRING,
#   LATITUDE           STRING
# )
                
# Replace `whoami` with your OS user name
# End of Problem

from pyspark.sql import Row

h1b_data_rdd = sc.textFile('/public/h1b/h1b_data_noheader'). \
filter(lambda rec: rec.split('\0')[7] != "NA"). \
filter(lambda rec: rec.split('\0')[6] != "NA"). \
map(lambda rec: Row(
	ID = int(rec.split('\0')[0]), 
	CASE_STATUS = rec.split('\0')[1], 
	EMPLOYER_NAME = rec.split('\0')[2], 
	SOC_NAME = rec.split('\0')[3], 
	JOB_TITLE = rec.split('\0')[4], 
	FULL_TIME_POSITION = rec.split('\0')[5], 
	PREVAILING_WAGE = float(rec.split('\0')[6]), 
	YEAR = int(rec.split('\0')[7]), 
	WORKSITE = rec.split('\0')[8], 
	LONGITUDE = rec.split('\0')[9], 
	LATITUDE = rec.split('\0')[10]
	))

h1b_data_DF = sqlContext.createDataFrame(h1b_data_rdd)

h1b_data_DF. \
write. \
saveAsTable('codersham.h1b_data')

#14 : Instructions

# Export h1b data from hdfs to MySQL Database

# Data Description

# h1b data with ascii character "\001" as delimiter is available in HDFS

# h1b data information:

# HDFS Location: /public/h1b/h1b_data_to_be_exported
# Fields: 
# ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
# Number of records: 3002373
# Output Requirements

# Export data to MySQL Database
# MySQL database is running on ms.itversity.com
# User: h1b_user
# Password: itversity
# Database Name: h1b_export
# Table Name: h1b_data_`whoami`
# Nulls are represented as: NA
# After export nulls should not be stored as NA in database. It should be represented as database null
# Create table command:

# CREATE TABLE h1b_data_`whoami` (
#   ID                 INT, 
#   CASE_STATUS        VARCHAR(50), 
#   EMPLOYER_NAME      VARCHAR(100), 
#   SOC_NAME           VARCHAR(100), 
#   JOB_TITLE          VARCHAR(100), 
#   FULL_TIME_POSITION VARCHAR(50), 
#   PREVAILING_WAGE    FLOAT, 
#   YEAR               INT, 
#   WORKSITE           VARCHAR(50), 
#   LONGITUDE          VARCHAR(50), 
#   LATITUDE           VARCHAR(50));
                
# Replace `whoami` with your OS user name
# Above create table command can be run using
# Login using mysql -u h1b_user -h ms.itversity.com -p
# When prompted enter password itversity
# Switch to database using use h1b_export
# Run above create table command by replacing `whoami` with your OS user name
# End of Problem

sqoop export \
--connect jdbc:mysql://ms.itversity.com/h1b_export \
--username h1b_user \
--password itversity \
--table h1b_data_codersham \
--export-dir /public/h1b/h1b_data_to_be_exported \
--input-null-string 'NA' \
--input-fields-terminated-by "\001"

#15 : Instructions

# Connect to the MySQL database on the itversity labs using sqoop and import data with case_status as CERTIFIED

# Data Description

# A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 3002373 rows of h1b data

# MySQL database information:

# Installation on the node ms.itversity.com
# Database name is h1b_db
# Username: h1b_user
# Password: itversity
# Table name h1b_data
# Output Requirements

# Place the h1b related data in files in HDFS directory
# /user/`whoami`/problem15/solution/
# Replace `whoami` with your OS user name
# Use avro file format
# Load only those records which have case_status as CERTIFIED completely
# There are 2615623 such records
# End of Problem

sqoop import \
--connect jdbc:mysql://ms.itversity.com/h1b_db \
--username h1b_user \
--password itversity \
--table h1b_data \
--where "case_status = 'CERTIFIED'" \
--target-dir /user/codersham/problem15/solution \
--as-avrodatafile

#16 : Instructions

# Get NYSE data in ascending order by date and descending order by volume

# Data Description

# NYSE data with "," as delimiter is available in HDFS

# NYSE data information:

# HDFS location: /public/nyse
# There is no header in the data
# Output Requirements

# Save data back to HDFS
# Column order: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
# File Format: text
# Delimiter: :
# Place the sorted NYSE data in the HDFS directory
# /user/`whoami`/problem16/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

nyse_data_rdd =  sc.textFile('/public/nyse'). \
map(lambda rec: Row(
	stockticker = rec.split(',')[0], 
	transactiondate = rec.split(',')[1], 
	openprice = float(rec.split(',')[2]), 
	highprice = float(rec.split(',')[3]), 
	lowprice = float(rec.split(',')[4]), 
	closeprice = float(rec.split(',')[5]), 
	volume = long(rec.split(',')[6])
	))

nyse_data_DF = sqlContext.createDataFrame(nyse_data_rdd)

from pyspark.sql.functions import col

result = nyse_data_DF. \
orderBy(col('transactiondate').asc(),col('volume').desc())

sqlContext.setConf('spark.sql.shuffle.partitions','1')

result. \
selectExpr("concat(stockticker,':',transactiondate,':',openprice,':',highprice,':',lowprice,':',closeprice,':',volume)"). \
write. \
text("/user/codersham/problem16/solution")

#17 : Instructions

# Get the stock tickers from NYSE data for which full name is missing in NYSE symbols data

# Data Description

# NYSE data with "," as delimiter is available in HDFS

# NYSE data information:

# HDFS location: /public/nyse
# There is no header in the data
# NYSE Symbols data with "\t" as delimiter is available in HDFS

# NYSE Symbols data information:

# HDFS location: /public/nyse_symbols
# First line is header and it should be included
# Output Requirements

# Get unique stock ticker for which corresponding names are missing in NYSE symbols data
# Save data back to HDFS
# File Format: avro
# Avro dependency details: 
# groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
# Place the sorted NYSE data in the HDFS directory
# /user/`whoami`/problem17/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

nyse_data_rdd =  sc.textFile('/public/nyse'). \
map(lambda rec: Row(
	stockticker = rec.split(',')[0], 
	transactiondate = rec.split(',')[1], 
	openprice = float(rec.split(',')[2]), 
	highprice = float(rec.split(',')[3]), 
	lowprice = float(rec.split(',')[4]), 
	closeprice = float(rec.split(',')[5]), 
	volume = long(rec.split(',')[6])
	))

nyse_data_DF = sqlContext.createDataFrame(nyse_data_rdd)

nyse_data_DF.registerTempTable('nyse_data')

nyse_symbols_with_header = sc.textFile("/public/nyse_symbols")

nyse_symbols_only_header = sc.parallelize([nyse_symbols_with_header.first()])

nyse_symbols_rdd = nyse_symbols_with_header.subtract(nyse_symbols_only_header). \
map(lambda rec: Row(
	symbol = rec.split('\t')[0], 
	description = rec.split('\t')[1]
	))

nyse_symbols_DF = sqlContext.createDataFrame(nyse_symbols_rdd)

nyse_symbols_DF.registerTempTable('nyse_symbols')

result = sqlContext.sql("select distinct stockticker from (select nyd.stockticker, nys.description from nyse_data nyd left outer join nyse_symbols nys on nyd.stockticker = nys.symbol)q where q.description is null")

sqlContext.setConf('spark.sql.shuffle.partitions','1')

result. \
write. \
format('com.databricks.spark.avro'). \
save('/user/codersham/problem17/solution/')


#18 : Instructions

# Get the name of stocks displayed along with other information

# Data Description

# NYSE data with "," as delimiter is available in HDFS

# NYSE data information:

# HDFS location: /public/nyse
# There is no header in the data
# NYSE Symbols data with tab character (\t) as delimiter is available in HDFS

# NYSE Symbols data information:

# HDFS location: /public/nyse_symbols
# First line is header and it should be included
# Output Requirements

# Get all NYSE details along with stock name if exists, if not stockname should be empty
# Column Order: stockticker, stockname, transactiondate, openprice, highprice, lowprice, closeprice, volume
# Delimiter: ,
# File Format: text
# Place the data in the HDFS directory
# /user/`whoami`/problem18/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

nyse_data_rdd =  sc.textFile('/public/nyse'). \
map(lambda rec: Row(
	stockticker = rec.split(',')[0], 
	transactiondate = rec.split(',')[1], 
	openprice = float(rec.split(',')[2]), 
	highprice = float(rec.split(',')[3]), 
	lowprice = float(rec.split(',')[4]), 
	closeprice = float(rec.split(',')[5]), 
	volume = long(rec.split(',')[6])
	))

nyse_data_DF = sqlContext.createDataFrame(nyse_data_rdd)

nyse_data_DF.registerTempTable('nyse_data')

nyse_symbols_with_header = sc.textFile("/public/nyse_symbols")

nyse_symbols_only_header = sc.parallelize([nyse_symbols_with_header.first()])

nyse_symbols_rdd = nyse_symbols_with_header.subtract(nyse_symbols_only_header). \
map(lambda rec: Row(
	symbol = rec.split('\t')[0], 
	description = rec.split('\t')[1]
	))

nyse_symbols_DF = sqlContext.createDataFrame(nyse_symbols_rdd)

nyse_symbols_DF.registerTempTable('nyse_symbols')

result = sqlContext.sql("select nyd.stockticker, nvl(nys.description,'') stockname, nyd.transactiondate, nyd.openprice, nyd.highprice, nyd.lowprice, nyd.closeprice, nyd.volume from nyse_data nyd left outer join nyse_symbols nys on nyd.stockticker = nys.symbol")

sqlContext.setConf('spark.sql.shuffle.partitions','1')

result. \
selectExpr("concat(stockticker,',',stockname,',',transactiondate,',',openprice,',',highprice,',',lowprice,',',closeprice,',',volume)"). \
write. \
text('/user/codersham/problem18/solution/')


#19 : Instructions

# Get number of companies who filed LCAs for each year

# Data Description

# h1b data with ascii null "\0" as delimiter is available in HDFS

# h1b data information:

# HDFS Location: /public/h1b/h1b_data_noheader
# Fields: 
# ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
# Use EMPLOYER_NAME as the criteria to identify the company name to get number of companies
# YEAR is 8th field
# There are some LCAs for which YEAR is NA, ignore those records
# Output Requirements

# File Format: text
# Delimiter: tab character "\t"
# Output Field Order: year, lca_count
# Place the output files in the HDFS directory
# /user/`whoami`/problem19/solution/
# Replace `whoami` with your OS user name
# End of Problem

sc.setLogLevel('ERROR')

from pyspark.sql import Row

h1b_data_rdd = sc.textFile('/public/h1b/h1b_data_noheader'). \
filter(lambda rec: rec.split('\0')[7] != 'NA'). \
map(lambda rec: Row(year = rec.split('\0')[7],employer_name = rec.split('\0')[2]))

h1b_data_DF = sqlContext.createDataFrame(h1b_data_rdd)

h1b_data_DF.registerTempTable('h1b_data')

result = sqlContext.sql("select year, count(distinct employer_name) lca_count from h1b_data group by year")

sqlContext.setConf('spark.sql.shuffle.parititons','1')

result. \
selectExpr("concat(year,'\t',lca_count)"). \
write. \
text('/user/codersham/problem19/solution/')

#20 : Instructions

# Connect to the MySQL database on the itversity labs using sqoop and import data with employer_name, case_status and count. Make sure data is sorted by employer_name in ascending order and by count in descending order

# Data Description

# A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 3002373 rows of h1b data

# MySQL database information:

# Installation on the node ms.itversity.com
# Database name is h1b_db
# Username: h1b_user
# Password: itversity
# Table name h1b_data
# Output Requirements

# Place the h1b related data in files in HDFS directory
# /user/`whoami`/problem20/solution/
# Replace `whoami` with your OS user name
# Use text file format and tab (\t) as delimiter
# Hint: You can use Spark with JDBC or Sqoop import with query
# You might not get such hints in actual exam
# Output should contain employer name, case status and count
# End of Problem

sqoop import \
--connect jdbc:mysql://ms.itversity.com/h1b_db \
--username h1b_user \
--password itversity \
--query "select employer_name, case_status, count(id) count from h1b_data where \$CONDITIONS group by employer_name, case_status order by employer_name asc, count desc" \
--target-dir /user/codersham/problem20/solution \
--fields-terminated-by '\t' \
--num-mappers 1
