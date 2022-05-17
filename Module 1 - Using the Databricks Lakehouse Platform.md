# Module 1 - Using the Databricks Lakehouse Platform

## Databricks Lakehouse Platform:
	1. Lakehouse is the "one simple platform to unify all of your data, analytics, and AI workloads”
	2. Over 5000 customers
	3. Original creators of Apache Spark, Delta Lake, mlflow

	Across many industries, currently servicing half of the Fortune 500

Most enterprises struggle with data and they have 4 main usage architectures:
	1. Data Warehousing
	2. Data Engineering
	3. Streaming
	4. Data Science and ML

![Screenshot 2022-04-26 at 16 39 55](https://user-images.githubusercontent.com/19376014/168785086-922ea54e-7a3c-4a69-90b7-89f5cd9355a7.png)


Main problems:

* Clients often have to cross maintain different stacks and make sure they work well together. Often this is costly and difficult to manage.
* Expensive to manage
* Complexity passed on to data teams
* Siloed teams
* No consistent security or data model
* Various versions of the truth

Lakehouse is the solution to solve for this complexity. Lakehouse is **one platform to unify all of your data, analytics, and AI workloads**. It combines the capabilities of the Data Lake and the Data Warehouse.

Lakehouse advantages:
		* fit for any size organisation
		* increasing simplicity
		* robust, secure, scalable
		* lowering TCO

**Delta Lake** at the heart of the Lakehouse:
		* Underpinned by Apache Spark
		* Better reliability with transactions
		* Ability to time travel backwards
		* Advanced caching and indexing methods, 48 times faster
		* Improved data governance capabilities to easily provision access to users

![](Module%201%20-%20Using%20the%20Databricks%20Lakehouse%20Platform/Screenshot%202022-04-26%20at%2016.51.03.png)

3 principles:
		* **Simple**: Unify your data, analytics, AI into one common platform for all data use cases. Data can be aggregated in one place.
		* **Open**: Unifying ecosystems, with open source standards and formats. Built on innovation of some of the most successful open source data projects.
			* Over 450 partners across the data landscape.
		* _[THIS IS OLD - THE NEW PILLAR IS MULTI-CLOUD]_ **Collaborative**: all 3 user profiles (Data Analyst, Data Engineer, Data Scientists) can collaborate in a single space to share:
			* Models
			* Dashboards
			* Notebooks
			* Datasets
		* **Multi-cloud**

- - - -
- - - -
- - - -

## Databricks Architecture and Services

![](Module%201%20-%20Using%20the%20Databricks%20Lakehouse%20Platform/Screenshot%202022-04-26%20at%2017.04.06.png)


2 Planes:
	1. **Control Plane**:
		* Databricks Cloud Account
		* Very little data resides here
		* Data is encrypted at rest
		* Allows access to:
			1. Web Application: Delivers 3 different services depending on the persona:
				1. Databricks SQL, for running quick, adhoc queries and visualisations
				2. Databricks Machine Learning, for integrated end to end machine learning environment - for tracking experiments, training models, managing feature development, and serving features and models
				3. Databricks Engineering and Data Science, also known as the workspace
			2. Repos/Notebooks
			3. Jobs
			4. Cluster Management, allows to manage the computational resources

**Clusters** are made up of one or more virtual machine instances.  A cluster will have a driver and multiple executors. The **driver** coordinates the activities of executors. **Executors** run tasks composing a spark job.  These jobs are performed using the cores, memory, and local storage available. 

![](Module%201%20-%20Using%20the%20Databricks%20Lakehouse%20Platform/Screenshot%202022-04-26%20at%2017.12.54.png)


There is a distinction between types of clusters:
		* 	**All-purpose clusters**: analyse data collaboratively using interactive notebooks
			* Can create using the Workspace or API
			* Retains up to 70 clusters for up to 30 days
			* Multiple users can use them
			* Can manually terminate and restart
		* **Job Clusters**: to run automated jobs in a robust and expeditious way
			* Job scheduler creates job clusters and terminates them when they are done
			* Can not restart a job cluster
			* Retain up to 30 clusters created by the job scheduler
			* To retain information past 30 days, the administrator must pin the cluster

- - - -

## Introduction to Delta Lake
What is Delta Lake:
	* 	Open source project that enables building a data Lakehouse on top of on existing storage system

It is NOT:
	* proprietary technology
	* storage format
	* storage medium
	* database service or data warehouse

It is:
	* open source
	* builds upon standard data formats
	* optimised for cloud object storage
	* built for scalable metadata handling

Brings ACID to object storage:
	* atomicity - all or nothing method of success/failure
	* consistency - predictable and expected behaviour - this is the one that varies the most between systems
	* isolation - how simultaneous operations are run. 
	* durability - committed changes are permanent

Most common problems that ACID solves for:

	1. Hard to append data
	2. Modification of existing data difficult
	3. Jobs failing mid way
	4. Real-time operations hard
	5. Costly to keep historical data versions

**Delta lake is the default for all tables created in Databricks**

### Managing Delta Tables

Creating Tables:
	* Providing table name as well as the columns and their types
```
CREATE TABLE table_name
	(column1 INT, column2 STRING, column3 DOUBLE)

--this will create a table with column names and column types defined *but* will fail if the table name already exists

CREATE TABLE IF NOT EXISTS table_name --will only create if table does not exist
	(column1 INT, column2 STRING, column3 DOUBLE)

CREATE OR REPLACE TABLE table_name --will create or replace if it exists
	(column1 INT, column2 STRING, column3 DOUBLE)
```

Inserting Data:
```
INSERT INTO table_name VALUES (1,"value_name",value); --can insert single values
INSERT INTO table_name VALUES (2,"value_name1",value1);
INSERT INTO table_name VALUES (3,"value_name2",value2);


INSERT INTO table_name -- can also insert mutiple values in a list format
	VALUES
		(4,"value_name3",value3),
		(5,"value_name4",value4),
		(6,"value_name5",value5);
```

Updating Data:
```
UPDATE table_name
SET value = value+1
WHERE name LIKE "T%"
```

Deleting Records:
```
DELETE FROM table_name
WHERE column > 5
```

Dropping a Table:
```
DROP TABLE table_name
```

Merging Tables:

Databricks uses **MERGE** as a way to upsert, which allows the ability to update, insert and apply other data manipulations as a single command.

**MERGE** statements must have at least one field to match on, and each **WHEN MATCHED** or **WHEN NOT MATCHED** clause can have any number of additional conditional statements.

```
MERGE table1 a
USING table2 b
ON a.column1=b.column1
WHEN MATCHED AND b.column2 = "value1"
	THEN UPDATE SET *
WHEN MATCHED AND b.column2 = "value2"
	THEN DELETE
WHEN NOT MATCHED AND b.column2 = "value3"
	THEN INSERT *
```


### Advanced Delta Lake Features

Optimize Client Docs:  [link](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html)
Vacuum Client Docs: [link](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html)

#### Table Details:
The below command allows to see important metadata about the table.
```
DESCRIBE EXTENDED table_name
```
![](Module%201%20-%20Using%20the%20Databricks%20Lakehouse%20Platform/Screenshot%202022-05-05%20at%2011.11.46.png)

It is important to take into account the location. Since this is a delta lake table, it is backed by a collection of files stored in cloud object storage.

Delta lake files will be stored as cloud objects and can be explored using the dbutils. The directory will contain a number of parquet data files and a directory named `_delta_log`. 

	Records in delta lake >>>> parquet files
	Transactions to delta lake files >>>> `_delta_log`

[Understanding the Delta Lake Transaction Log - Databricks Blog](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)

`DESCRIBE DETAIL table_name` will actually provide other useful details about the delta table, including the number of files. Delta lake will use the transaction log to indicate whether or not files are valid in a current version of the table.

Looking at the log files using a command such as below, we can see the changes committed against a table:

```
%python
display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))
```
![](Module%201%20-%20Using%20the%20Databricks%20Lakehouse%20Platform/Screenshot%202022-05-05%20at%2011.20.31.png)

#### Optimize
Small files may occur for a variety of reasons based on the operations performed. In order to combine these files towards the optimal size (scaled based on the total size of the table), we can use the `OPTIMIZE` command.

```
OPTIMIZE table_name
ZORDER BY column1
```

#### Transactions and History
Because all the transactions are stored in the transaction log, it’s possible to view the table history using the command below:

```
DESCRIBE HISTORY table_name
```

The above will show all the versions of the table and it is possible to view and restore a specific version.

Viewing a previous version:
`SELECT * FROM table_name VERSION AS OF 1`

Restoring a previous version:
`RESTORE TABLE table_name TO VERSION AS OF 5`

#### Vacuum
Although Databricks will automatically clean up stale files in Delta Lake tables, it is possible to manually delete/clean-up  versions. The `VACUUM` operation allows to do this and the retention policy of time can be specified with `0 HOURS`. Therefore:

```
VACUUM table_name RETAIN 0 HOURS
```

By default, VACUUM will prevent you from deleting files less than 7 days old - reason for this being that there may be dependencies for long-running tasks on these files. 

It is possible to disable the prevention of premature deletion via the below:
`SET spark.databricks.delta.retentionDurationCheck.enabled = False`

Logging can be enabled via:
`SET spark.databricks.delta.vacuum.logging.enabled = True`

And you can perform a `DRY RUN` to print out all records to be deleted:
`VACUUM table_name RETAIN 0 HOURS DRY RUN`

#training/data engineering cert#
