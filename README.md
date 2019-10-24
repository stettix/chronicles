# Chronicles

This project contains code that allows big data tables to be modified in a safe way, tracking previous versions of data and performing updates in a way that is fast and reliable.

For a detailed discussion about the origin of this project, see [the documentation](/docs/concepts.md).

## Why use Chronicles

Big data pipelines often store data directly for example as Parquet or ORC files, instead of writing to a databases.
Such files are often written to HDFS file systems, or cloud blob storage such as AWS S3, Google Cloud Storage, or Azure Blob Storage.

The drawback of writing such data directly as files is that you don't have database features such as transactions.
Updates to data require deleting old data as well as writing new data, and these updates are not atomic.
This means you can get wrong results while reading or querying the data while such operations take place.

This also means there is no easy way to revert to a previously known good state if a write goes wrong.

Such problems are not so much a problem when datasets are append-only, i.e. only new data is written out.
However, we find that in practice, updating existing data is often required, for example to reprocess data after fixing bugs, or when optimising the storage of files within a dataset.

Chronicles allows all updates to be performed safely, by using an immutable storage schema for data, and tracking the history of datasets hence knowing which files to include in each version of the data.

This also means you get an audit history of all updates to a dataset.
And, you can even have concurrent views on different versions of a given table.
In other words, you can query an older version of a table while production users are accessing the latest version of the table.

## Why not use Chronicles

This project is a toolkit that requires some self-assembly, i.e. it's not a shrink-wrapped solution that you can point and click to install in your data lake.

The project is at an early stage: while the basic functionality is complete and tested, it has _not_ been battle hardened in production use yet.
If you're considering using this in production feel free to [get in touch](TODO) for help.

If you want the security of a solution backed by a large organisation, you may want to consider one of the [alternatives](#alternatives) instead.

## Status

The basic functionality of this project is in place, but this has not been used in production systems yet.
We expect to do the initial release of the software artifacts for this in the near future.

## Limitations

It's worth being aware of the following limitations of Chronicles:

* Chronicles does not support record based updates, only updates by partition.
This may or may not impact the performance for your use case.
* It does not explicitly support schema evolution.
Non-breaking schema changes can be done as normal, but there is no special provision for handling breaking schema changes.
To perform schema-breaking changes, you still need to populate a new table from scratch.
* If you want to limit the amount of storage space used for older version, you will have to have automated processes in place that remove older versions of data based on some criteria such as age or the number of kept versions.
This is typically needed for data partitioned by time anyway, but the logic of such purging of old data will have to change to take into account stored versions.


## Alternatives

The following tables lists some alternatives to Chronicles, highlighting the differing functionality they provide.


| &nbsp;  |             Chronicles      |  Apache/Netflix Iceberg      | Databricks Delta Lake     |
| ------------- | -------------   | :----------------------- | :------------------- |
|**General features**||||
|Is open source                          | ✅ | ✅ | ✅ (some functionality excluded)|
|Atomic updates                          | ✅ | ✅ | ✅|
|Row level updates                       | ❌ | ✅ | ✅|
|Schema evolution                        | ❌ | ✅ | ✅|
|Requires migrating data on adoption     | ✅ | ❌ | ❌|
|Requires migrating metastore on adoption| ✅ | ❌ | ❌|
|**Data write support** ||||
|Supports Spark     | ✅ | ✅ | ✅|
|Supports Flink     | ✅ | ❌ | ❌|
|Data read support||||
|Supports Spark     | ✅ | ✅ | ✅|
|Supports Flink     | ✅ | ❌ | ❌|
|Supports Presto    | ✅ | ✅ | 🔸 (limited support, not in open source edition)|
|Supports AWS Athena| ✅ | ✅ | 🔸 (limited support, not in open source edition)|
|Supports HiveSQL   | ✅ | ❌ | ❌|
|**File format support**||||
|Parquet            | ✅ | ❌ (uses custom format) | ✅ (uses Parquet but a custom storage layout)|
|ORC                | ✅ | ❌ | ❌|
|Avro               | ✅ | ❌ | ❌|
|CSV                | ✅ | ❌ | ❌|
|**Metastore support**||||
|AWS Metastore      | ✅ | ✅ | ❌|
|Hive Metastore     | ✅ | ✅ | ❌|
|Custom metastore   | ❌ | ❌ | ✅|

As this table highlights, an important difference between Chronicles and the alternatives is that Chronicles stores data in a way that is fully compatible with any current or future tool or system that can query via a Metastore.
The other options define custom file formats or use a custom directory layout that means any tool used to write to or query the data needs to have explicit support for these custom formats.

The approach taken by Chronicles does limit its functionality in some ways, it can for example not do row-level updates.
How serious this limitation is depends on your exact use case.

## License

This project is released under the [Apache 2 License](/LICENSE)

## Acknowledgements

This project is based on a [prototype](https://github.com/guardian/table-versions) built at the Guardian.
Many thanks to the Guardian's Data Tech team and overall Engineering team for the support!
