# Module 4: Managing Data Access and Production Pipelines
### Unity Catalog

4 key functional areas:
1. Data access control - control who has access to data
2. Data access audit - capture and record all access to data
3. Data lineage - to capture upstream sources and downstream consumers
4. Data discovery - ability to discover and search assets

<img width="909" alt="Screenshot 2022-05-09 at 19 49 33" src="https://user-images.githubusercontent.com/19376014/169256577-33233e8b-0d75-4029-b8d3-21864325a578.png">

Aim is to:
* Unify governance across clouds - based on open standard ANSI SQL
* Unify data and AI assets
* Unify existing catalogs

Instead of a two layer namespace - `SELECT * FROM schema.table`, it will become a three layer namespace - `SELECT * FROM catalog.schema.table`.

<img width="917" alt="Screenshot 2022-05-09 at 19 53 07" src="https://user-images.githubusercontent.com/19376014/169256700-e85ee181-b0a7-41ce-921b-401177dd44bb.png">


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
