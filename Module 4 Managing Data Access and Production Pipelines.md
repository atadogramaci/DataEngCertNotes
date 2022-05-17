# Module 4: Managing Data Access and Production Pipelines
### Unity Catalog

4 key functional areas:
	1. Data access control - control who has access to data
	2. Data access audit - capture and record all access to data
	3. Data lineage - to capture upstream sources and downstream consumers
	4. Data discovery - ability to discover and search assets

![](Module%204%20Managing%20Data%20Access%20and%20Production%20Pipelines/Screenshot%202022-05-09%20at%2019.49.33.png)

Aim is to:
	* Unify governance across clouds - based on open standard ANSI SQL
	* Unify data and AI assets
	* Unify existing catalogs

Instead of a two layer namespace - `SELECT * FROM schema.table`, it will become a three layer namespace - `SELECT * FROM catalog.schema.table`.

![](Module%204%20Managing%20Data%20Access%20and%20Production%20Pipelines/Screenshot%202022-05-09%20at%2019.53.07.png)


### Managing Permissions

In relation to access layers, access can be granted to the below layers:
	* CATALOG
	* DATABASE
	* TABLE
	* VIEW
	* FUNCTION
	* ANY FILE - grants access to the underlying file system, must be noted that users granted access to the ANY FILE can bypass the database components and access files directly

Databricks admins and object owners can grant access according to the following roles:
	* Databricks administrator - all objects in the catalog and underlying filesystem
	* Catalog owner - all objects in the catalog
	* Database owner - all objects in the database
	* Table owner - only the table

The levels of privileges are:
	* ALL PRIVILEGES
	* SELECT
	* MODIFY
	* READ_METADATA
	* USAGE
	* CREATE


#training/data engineering cert#
