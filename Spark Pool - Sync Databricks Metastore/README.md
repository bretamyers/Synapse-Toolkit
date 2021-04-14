# Sync Databricks Metastore

This script will connect to a Databricks cluster and pull in the hive metastore artifacts to recreate them within spark pools. Since a spark pools metastore is sync'd with the serverless pool, you can then query the data using the serverless tsql syntax.

The tables must be external tables within Databricks. For the metadata to be sync'd to serverless, the data must also be parquet format and not partitioned.
