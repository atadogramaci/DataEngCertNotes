# Module 3: Incremental Data and Delta Live Tables
Incremental ETL is important because it allows to process solely the new data that has been encountered. 

Databricks Autoloader provides a simple way of incrementally loading data to a data lake. It is recommended as **best practice** to use when ingesting data from cloud object storage.

Autoloader will also automatically add a `_rescued_data` column. This column will denote when a record was not updated into the table for some reason and it will be quarantined automatically, so that it can be reviewed manually later on and inserted.

[Auto Loader | Databricks on AWS](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html)

There are 4 minimum arguments that should always be specified”
	
1. `data_source`: this is the directory of the source and autoloader will detect new files as they land. Passed on to the `.load()` method.
2. `source_format` is the data format. The format of the source files should always be specified as `cloudFiles.format` option.
3. `table_name` is the target table name.  Spark Structured Streaming supports writing directly to Delta Lake tables by passing a table name as a string to the `.table()` method. Note that you can either append to an existing table or create a new table.
4. `checkpoint_directory` will be the location for storing metadata about the stream. -> using the `cloudFiles.schemaLocation` option. 

```
query = (spark.readStream
				.format("cloudFiles")
				.option("cloudFiles.format", source_format)
				.option("cloudFiles.schemaLocation", checkpoint_directory)
				.load(data_source)
				.writeStream
				.option("checkpointLocation", checkpoint_directory)
				.option("mergeSchema", "true")
				.table(table_name))
```

[Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts) allows users to interact with ever-growing data sources as if they were a static table.

New data in the data stream is added into the table as if they were new rows appended to an unbounded table. 

<img width="775" alt="Screenshot 2022-05-09 at 19 25 41" src="https://user-images.githubusercontent.com/19376014/169248792-a9c79494-4d33-4f26-9af0-a409bd24d8e1.png">

This is advantageous because the table does not need to be reprocessed for each update. The addition logic is only applied to the new data coming in.

Spark Structured Streaming is optimised on Databricks to integrate with Delta Lake.

Basic concepts of the structured streaming follows that:
	
* Developer defines an **input table** by configuring a streaming read against a **source**.
* A **query** is defined against the input table.
* This logical query on the input table will generate the **results table**.
* The output of a streaming pipeline will persist updates to the results table by writing to an external **sink**. Sing will generally be a durable system such as files or a pub/sub messaging bus.
* New rows are appended to the input table.

Structured streaming ensures **end-to-end, exactly once semantics** under any failure conditions. The underlying streaming mechanism relies on:
	
* using checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval
* streaming sinks are designed to be idempotent (meaning that multiple writes do not cause duplicates being written to the sink)

The basic syntax on reading a stream is:
```
(spark.readStream
		.table("bronze")
		.createOrReplaceTempView("streaming_tmp_view"))
```

If any queries are done against this table, it will then update in a streaming fashion once there are updates. Queries against streaming temp view will be **always-on incremental queries**.

In order to stop a query on a streaming temp view, user has 3 options:

* Clicking on “cancel” In the UI
* Clicking on “Stop Execution” in the UI
* Using the following code:
		* Leveraging `spark.stream.active` to view all active streams
		* Then invoking the `.stop()` method and then `.awaitTermination()`.

```
for s in spark.streams.active:
		s.stop()
		s.awaitTermination()
```

### Unsupported Operations

Although most operations on a streaming DataFrame are identical, there are certain operations that are not supported.  One of these is ordering a streaming table. [Structured Streaming Programming Guide - Spark 3.2.1 Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations)

### Writing a Stream

To persist the results of a streaming query, it must be written to a durable storage, which can be done by the `DataFrame.writeStream` method. There are 3 crucial components to configuring a writeStream call:
		
1. Checkpointing:
	* Stores the current state of your streaming job to cloud storage
	* Combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off
	* Cannot be shared between separate streams
2. Output Mode: defining between:
	* Append - `.outputMode("append")` - default mode and only newly appended rows are incrementally appended to the target table with each batch
	* Complete - `outputMode(“complete”)` - results table is recalculated each time a write is triggered and the target table is overwritten with each batch.
3. Trigger intervals:
	* Unspecified: default mode - equivalent to `processingTime=“500ms”`, every half a second
	* Fixed interval micro-batches - `trigger(processingTime=“2 minutes”)` - this query will be executed and kicked off at the specified intervals
	* One-time micro-batch - `.trigger(once=True)` - execute once to process all available data and stop on its own.

# Medallion Architecture in the Data Lakehouse
<img width="877" alt="Screenshot 2022-05-15 at 16 31 33" src="https://user-images.githubusercontent.com/19376014/169249305-9c24e9ba-6e35-459c-8956-1422787885b8.png">

3 layers and data quality generally increases as it moves through this process layers:

* Bronze: 
	* copy of raw ingested data
	* replaces the traditional data lake
	* efficient storage and querying of full, unprocessed history of data
* Silver:
	* Validated, enriched version of the data that can be trusted for analytics
	* Reduces data storage complexity, latency, and redundancy
	* Preserves grain of original data
	* Validated single source of truth of the bronze data
	* Eliminates duplicate records, has had quality checks applied, with bad records quarantined
	* Production schema enforced
* Gold:
	* Highly refined and validated
	* Powering ML applications, reporting, dashboards, ad hoc analytics
	* Refined views of data, typically with aggregations
	* Optimises query performance for business-critical data

## Delta Live Tables
Although the medallion architecture simplifies the data ingestion pipeline, the reality for firms can still be complex as they need to maintain multiple tables on various layers with complex dependencies.

Delta Live Table has been designed to make reliable ETL easy:
	* Operating with agility - declarative tools to build batch and streaming data pipelines
	* Built-in quality controls - having the ability to declare quality expectations and taking actions
	* Scaling with reliability - to easily scale infrastructure alongside the data
	
