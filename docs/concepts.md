# Architecture and key concepts

## Background

### Data warehouses

Traditional data warehouses typically consist of a large analytical database, populated by a large number of ETL (Extract Transform and Load) jobs.
These read data from external sources, process (transform) these, then load data into the database for querying.

A disadvantage of this approach is the cost and complexity of managing the database at the heart of the data warehouse.
The storage used by it needs to be carefully managed, and increased to cope with increased data volumes.
Data needs to be backed up to ensure no data is lost.
And, as the database cluster is accessed by all jobs that use data, whether they write or query data, it can easily become a bottleneck and a limitation on performance.
Scaling up database clusters to cope with growth in demand is expensive and requires dedicated teams of skilled operators.

### The "Big Data" approach

As a result of these challenges, another approach has emerged since cloud computing became commonplace.
Instead of storing all data in the central database, data is stored on highly available filesystems (such as HDFS), or in cloud storage, in other words storage that is managed by the cloud platform provider.
These approaches rely on storing data on cheap commodity hardware, and relying on data replication (redundant copies) to provide very high availability and reliability.
Additionally, data is stored in independent volumes without support for foreign keys that link different tables together.
This decoupling of storage for separate datasets/tables makes it much easier to scale up storage capacity.
Also, access to data does not need to go via a central database cluster, making it much easier to scale up performance, avoiding bottlenecks under heavy load for example.

The data that's stored in this way can be read by ETL jobs, but solutions have also appeared for running SQL queries on this data in-place.
That means that many use cases can be served directly from data in cloud storage.
In other words, it may not be necessary to load all the data into a database for serving data to end users running ad-hoc SQL queries.
Examples of technologies that allows querying data directly from cloud storage include [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive), [Presto](http://prestodb.github.io), [AWS Athena](https://aws.amazon.com/athena/), and [Apache Spark](Spark).
These solutions can scala up to handle queries on very large datasets, although the performance of queries depends a great deal on the details of how data has been stored (e.g. partitioning schemes and sorting).
Hence it may still be useful to load select datasets into databases for queries and usage patterns that can't be served well directly from storage.

### Challenges with the "Big Data" approach

When storing data directly, we're essentially dealing with raw files instead of a database.
This means that we lose some key features of databases, first and foremost the notion of __transactions__.
As a result, data writes are no longer atomic.
__Updates__ are particularly problematic.
When updating existing data, we have to delete the old version of the data as well as add the new data.
As data in tables is usually stored in a large number of distinct files, it's not possible to do this in an atomic way.
This means that anyone querying the table while it's being updated will either see old data and new data at the same time, or if the deletions happen first, they may see data disappaering only to reappear when the writes complete.
This means there's a risk of getting completely wrong query results for queries that run while a table is updated.
Even worse: if the update fails part-way through, then the table will be left in a corrupt state, with partial written and/or deleted data.

This is the problem that Chronicles aims to fix.

## How Chronicles provides the best of both worlds

### Immutable storage and version history

The key insight behind Chronicles is that all data that's written should be **immutable**.
The problems described above with the Big Data approach all trace back to the fact that data is shared and mutable.
And we all know that shared mutable state is the root of all evil!

Chronicles builds on all the existing Big Data technologies, but it writes data so that once written, data never changes.
Specifically, Chronicles writes each __partition__ as an immutable slice of data.
Then, instead of overwriting or deleting data, it writes new versions of each partitions and keeps track of which versions that belong to each version of the table as a whole.

### Integration with Big Data technologies

In Big Data systems, data is stored as plain files.
To make sense of this data, and to make it easily readable for queries etc., we typically use a __Metastore__ to track the metadata about the raw data.
This contains information about which file format is used, which columns to expect and their types, as well as the location of data for each partition.
The [Hive Metastore](https://docs.cloudera.com/documentation/enterprise/5-8-x/topics/cdh_ig_hive_metastore_configure.html) is the prototypical metatsore, but other variations exist, such as the [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html).

When using Chronicles, data is still written as before, using the same file formats.
The only thing that changes is that a new version is written every time a partition is written, and that the Metastore is updated so it points to the right data location.
Hence Chronicles is 100% compatible with all current and future tools and frameworks that suport querying file data via a Metastore.

# Benefits of Chronicles

The approach taken by Chronicles ensures that all data updates are atomic, hence tables can be updated in a transactional manner.
We do this while keeping all the benefits that Big Data technologies provide, such as great scalability.
Updates to data do not take effect until it is complete.
If a write fails part way through updating a table, then the existing view of the data is completely untouched.
There is no window of opportunity for race conditions, as no deletes are taking place: new data is written then the table definition in a Metastore is updated on commit so the changeover is atomic and near-instantaneous.

There are a number of further advantages that come as a result of the way Chronicles tracks updated versions of partitions:

* Audit trail: Chronicles stores the history of updates, which means that we get a log showing how each table has changed over time.
* Reverting to older table versions: As old partition data is never overwritten, we can use the version information tracked in Chronicles to update the current view of a table to point to any previous version of data.
Reverting to an older version of data does **not** move or copy __any__ data, it only entails updating the metadata in a Metastore, hence is very fast.
This means that if data in a table is corrupted, for example by a bug, we can very quickly revert the data to a previous version, without performing any re-processing of the data.
* We can create new tables that are based on arbitrary versions of a production table.
For example, if the latest version of data looks suspicious, you can simply create a new debug table that refers to an older version of the table.
This can then be used for arbitrary queries against the latest data, for example finding differences.
Put simply: you can see what a table looked like at any point in its history, without affecting the live version of the table used in production.

One way to summarise these features is that Chronicles provides version control for your big data!
This is not an accident, as version control systems like [`git`](https://git-scm.com/book/en/v1/Getting-Started-About-Version-Control) is a major inspiration for Chronicles.

Also, as mentioned above, the that Chronicles integrates at the Metastore level means that it is fully compatible with all tools and frameworks that support querying table data via a Metastore.
This means you can keep using existing file formats (Parquet, ORC, Avro etc) - or use new ones as they appear!
Likewise, all tools for querying the data will work (e.g. Hive, Spark, Flink, Presto, Athena) will work.
Hence when you migrate tables to Chronicles, all downstream users and systems that query your data do not need to change at all!

Another potential benefit is that it becomes easier to implement backup solution for data stored in Chronicles.
The reason is that data is only ever __written__ to storage, never deleted or updated.
This means that it is only necessary to replicate these writes to another storage system in order to back up the full history of a table.
If you're familiar with `git`, you may think of this as cloning a repository to a different location, pulling in changes as they happen in the master repository.
Backing up a regular mutable table by comparison is complex, and raises question such as at which frequencies tables should be backed up, how to store the history of backed up versions and so on.

# Summary

Big Data technologies provide many benefits, first and foremost great scalability, but also a vibrant ecosystem of tools for working with such data.
Chronicles builds on these technologies while rectifying some of its limitations, adding transactional updates, auditability, and version control for big datasets.
