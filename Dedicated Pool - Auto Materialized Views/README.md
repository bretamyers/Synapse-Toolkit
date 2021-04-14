# Auto Materlized Views

This script will read from a dedicated pools DMVs to get a list of the top queries that have the longest duration. We then loop through the queries and add the 'EXPLAIN WITH_RECOMMENDATION' to the query to get the suggested materialized view create statement. We take that statement and attempt to create the materialized view in the dedicated pool.

This should be used with great caution.
