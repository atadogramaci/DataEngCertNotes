# Module 2: ETL with Spark SQL and Python
## Relational Entities on Databricks
### Databases

It’s possible to create a database with or without specifying a location. If not specified, the database will be created under `dbfs:/user/hive/warehouse/` .

Database without location specified:
`CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;`

Database with location specification:
`CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION ‘${da.paths.working_dir}/_custom_location.db’;`

To view details of a database, run:
`DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location`


There are two types of tables:

1. Managed Table: this is a Spark SQL table for which Spark manages both the data and the metadata. The metadata and data is stored in DBFS in customer account. Dropping the table will remove both the metadata and data.
2. Unmanaged Table: the alternative where Spark will manage the metadata but the data location is controlled by the user. When a drop table is performed, only the metadata is removed and not the data itself. The data will continue to live in the specified location. 

Therefore the content replacement of tables will differ across both. There needs to be more commands executed in the unmanaged tables. 

The main difference therefore is around the files associated with managed tables. When a managed table is dropped, the files associated will be deleted when dropped, since they are stored on the root DBFS storage linked to the workspace. 

**External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.** 

In order to create tables within a database:

```
USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
```

### View, Temp Views & Global Temp Views
[Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html) 
[Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables) 
[Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui) 
[Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table) 
[Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables) 
[Create View - Databricks Docs](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html) 
[Common Table Expressions - Databricks Docs](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html) 

A view does not create a table but it will still show under `SHOW TABLE` command. A temp view/table will not be registered against a database, whereas a view will. The view is a definition of the instructions required to show the desired outcome.

```
CREATE VIEW view_delays_abq_lax AS 
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;
```

The above can be modified to create temp as well as global temp views by using `CREATE TEMP VIEW` or `CREATE GLOBAL TEMP VIEW`.

Tables and views attached to databases will continue to exist. However, temp views will not exist once a kernel is shut down. A global temp view will continue to exist in a special **global temp** database. The `SHOW TABLES` command will not show global temp views but they will be assigned to global_temp database.


## ETL with Spark SQL
### Data Parsing
To query a single file:
```
SELECT * FROM file_format.`/path/to/file`
```
Important to note thatches are back-ticks around the file path and not single quotes.

The above can be used to query a directory as well and it will pull in all of the files in that directory for that format:

Single file:
```
SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`
```
Directory of files:
```
SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka`
```


Alternatively, it’s possible to extract the raw strings, raw bytes and metadata of a file.
Extract text as raw string:
```
SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`
```
Extract raw bytes and metadata:
```
SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`
```
(Path,  modificationTime, length, content columns will be added)

### Options for External Sources

Spark supports many [file sources](https://docs.databricks.com/data/data-sources/index.html) and there are a variety of [additional configurations](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html).
A common format and example is:
```
CREATE TABLE table_identifier (col_name1 col_type1, ...)
USING data_source
OPTIONS (key1 = val1, key2 = val2, ...)
LOCATION = path
```

Extracting data from SQL:
```
CREATE TABLE
USING JDBC
OPTIONS (
    url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",
    dbtable = "{jdbcDatabase}.table",
    user = "{jdbcUsername}",
    password = "{jdbcPassword}"
)
```
_If running on a cluster with multiple workers, the client running the executors will not be able to connect to the driver. Back-end config of JDBC server assume the notebook is running in a single-node cluster._

Spark automatically caches the underlying data in local storage, in order to ensure that Spark provides optimal performance in subsequent queries, it’s possible to invoke a manual refresh of the cache via:
`REFRESH TABLE table_name`

Working with large external SQL databases can incur significant overhead because of:
1. Network latency with moving data over internet
2. Execution of query logic not being optimised in source systems

## Creating Delta Tables

CTAS = CREATE TABLE __ AS SELECT statements create and populate delta tables based on the source specified. These infer schema and do not allow manual schema declaration. They also do not support additional file options, which means that in if a user wanted to specify options for parsing sources, they would have to create a reference to the file:

```
CREATE OR REPLACE TEMP VIEW temp_view_name
	(key1 INT, key2 STRING, ...)
USING CSV
OPTIONS (
	path "${da.paths.datasets}/path/to/file"
	header "true"
	delimiter "|"
);

CREATE TABLE table_name AS
SELECT * FROM temp_view_name;
```

Columns can be renamed and filtered during the `SELECT` portion of CTAS:
```
CREATE OR REPLACE [TABLE/VIEW] table/view_name AS
SELECT key1 AS renamed_key1, key2
FROM original_table;
```

A column can be generated as part of this as well as declaring the schema. If a column type is not provided, then Delta Lake will automatically compute it.

### Table Constraints

Databricks supports two types of constraints:
	1. NOT NULL
	2. CHECK

Once a constraint is added, data violating the constraint will result in write failure. To add a constraint:
```
ALTER TABLE table_name ADD CONSTRAINT constraint_name CHECK (key1 =<> value)
```

Table constraints will be shown in the TBLPROPERTIES field and visible in `DESCRIBE EXTENDED table_name`.

### Enriching Tables
The **SELECT** clause can leverage built-in Spark SQL commands useful for file ingestion such as:

* **current_timestamp()** records the timestamp when the logic is executed
* **input_file_name()** records the source data file for each record in the table

The **CREATE TABLE**  also supports contains several options such as :

* `COMMENT` which adds a comment
* `LOCATION` which will result in an external table
* `PARTITIONED BY` which will cause the data to exist within its own directory in the target storage location

Most delta lake tables (especially small-to-medium sized data) will not benefit from partitioning. This is because it physically separates data files and can result in small file problems. _As best practice, default to non-partitioned tables for most use cases._

### Cloning Delta Lake Tables
Two ways of copying delta lake tables:

1. DEEP CLONE: fully copies data and metadata from a source table to a target. Incremental copy.
2. SHALLOW CLONE: only copies the delta transaction logs, and the data does not move. This can be useful for quick testing of changes without the risk of modifying the current table.

## Writing to Delta Tables

Users have 4 options for operations when it  comes to writing into delta tables;

1. `INSERT OVERWRITE`
	* This will overwrite the contents, replacing them. A fast operation as it does not list the directory recursively  or delete 
	* Previous version will be available through Time Travel
	* Atomic operation, meaning that concurrent queries can still read the table while being deleted
	* ACID guarantees that if operation fails, the previous state will be used
	* Unlike the CRAS (create or replace table as select), this query can only overwrite an existing table, it can not create a new one
	* Can only overwrite with new records that match the current table schema
	* Can overwrite individual partitions
	* ex: `INSERT OVERWRITE table_name SELECT * from source`
2. `INSERT INTO`
	* Appends records, allows fort incremental updates to existing tables
	* There is no built-in guarantee from appending duplicate records
	* ex: `INSERT INTO table_name SELECT * from source`
3. `MERGE INTO`:
	* Updates, inserts, and deletes are treated as a single transaction
	* Multiple conditions can be added in addition to matching fields
	* Extensive options for implementing custom logic
	* ex: `MERGE INTO target a USING source b ON {merge condition} WHEN MATCHED THEN {matched action} WHEN NOT MATCHED THEN {not_matched_action}`
4. `COPY INTO`:
	* Idempotent option for incrementally ingesting data
	* Data schema has to be consistent
	* Duplicate records should try to be excluded or handled downstream
	* Cheaper than full table scans for data
	* Real value is in multiple executions over time picking up new files in the source automatically


## Cleaning Data

### Inspecting the Data

* `count(col)` will count the number of populated items in a column. It will skip the **NULL** when counting.
* `count(*)` will count the total number of rows, including the *NULL* values.
* `count_if(col IS NULL)` to count the **NULL** values in a cell.
* `DISTINCT` will isolate the unique occurrences

_Spark skips null values while counting values in a column or counting distinct values for a field, but does not omit rows with nulls from a _**_DISTINCT_**_ query._

* `GROUP BY` can be used to aggregate records
* `date_format(column, “HH:mm:ss”)` will extract and create a date formatted column
* `regexp_extract` to extract characters/string from a column

## Advanced SQL Transformations
Spark SQL can traverse nested JSON data within columns with use of “:”, such as:

* `SELECT column:value:value1`- this will traverse in the column that is selected and find the key of value within the dictionary and then again the value1 nested key
It can also parse JSON objects into struct types. In order to do this, it is possible to utilise the `schema_of_json` to infer the schema as `from_json` requires a schema to be passed:

```
CREATE OR REPLACE TEMP VIEW parsed_events AS
  SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
  FROM events_strings;
  
SELECT * FROM parsed_events
```

Output:
![Screenshot 2022-05-09 at 14 44 05](https://user-images.githubusercontent.com/19376014/169248458-b0636cb9-1c9a-4327-908b-230e6fbc0538.png)

Spark SQL also makes use of the DESCRIBE for complex and/or nested structures.

Other data manipulations for nested structures:
	
* `explode` function lets us put each element in an array on its own row
* `collect_set` can collect unique values for a field, including fields within arrays
* `flatten` allows multiple arrays to be combined into a single array
* `array_distinct` removes duplicate elements from an array

Set Operators:

* `UNION` is joining two tables
* `INTERSECT` returns records that occur in both tables
* `MINUS` returns records found in one dataset but not the other.

### Higher Order Functions

Higher order functions in Spark SQL allow you to work directly with complex data types. When working with hierarchical data, records are frequently stored as array or map type objects. Higher-order functions allow you to transform data while preserving the original structure.
Higher order functions include:

* `FILTER` filters an array using the given lambda function: 
	* `FILTER (items, I -> i.item_id LIKE “%K”) AS king_items`
		In the statement above:
		* `FILTER` : the name of the higher-order function
		* `items` : the name of our input array
		* `I` : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.
		* `->` : Indicates the start of a function
		* `i.item_id LIKE “%K”` : This is the function. Each value is checked to see if it ends with the capital letter K. If it is, it gets filtered into the new column, **king_items**

* `EXIST` tests whether a statement is true for one or more elements in an array.
* `TRANSFORM` uses the given lambda function to transform all elements in an array:
	* `TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`
* `REDUCE` takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer.

## SQL UDFs and Control Flow
Databricks supports User Defined Functions registered natively in SQL. In order to register a function:

```
CREATE OR REPLACE FUNCTION function_name(text STRING)
RETURNS STRING
RETURN function command(input_value, output_value)
```

A user can run the DESCRIBE on the function to understand the scope and the permissions:
```
DESCRIBE FUNCTION function_name
-- the above will show basic information about the function

DESCRIBE FUNCTION EXTENDED function_name
-- the above will show more information including the SQL logic used in the function itself
```

CASE/WHEN query can be used to implement an if logic to implement control flow.

```
CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE 
  WHEN food = "beans" THEN "I love beans"
  WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
  WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
  ELSE concat("I don't eat ", food)
END;
```
