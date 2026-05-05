{% docs src_redshift_system_sources %}
The [System Monitoring Views](https://docs.aws.amazon.com/redshift/latest/dg/serverless_views-monitoring.html)
provided by AWS to monitor Redshift resources. These sources are all materialized as views by AWS.
{% enddocs %}

{% docs redshift_query_history %}
[Redshift query history view documentation](https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_HISTORY.html).
This view is available to all users. Superusers will be able to see all queries while regular users can only see their own queries.
This system view is recommended over the legacy stl_query as stl_query does not support serverless.
{% enddocs %}

{% docs redshift_users %}
[Redshift users view documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_USER_INFO.html).
{% enddocs %}

{% docs redshift_serverless_usage %}
[Redshift serverless usage view documentation](https://docs.aws.amazon.com/redshift/latest/dg/SYS_SERVERLESS_USAGE.html).
This view contains the serverless usage summary including how much compute capacity is used to process queries and the amount of Amazon Redshift managed storage used at a 1-minute granularity. The compute capacity is measured in Redshift processing units (RPUs) and metered for the workloads that you run in RPU-seconds on a per-second basis. RPUs are used to process queries on the data loaded in the data warehouse, queried from an Amazon S3 data lake, or accessed from operational databases using a federated query. Amazon Redshift Serverless retains the information in SYS_SERVERLESS_USAGE for 7 days.
{% enddocs %}

{% docs redshift_query_detail %}
Use SYS_QUERY_DETAIL to view details for queries at various metric levels, 
with each row representing details about a particular WLM query at a given metric level. 
This view contains many types of queries such as DDL, DML, and utility commands 
(for example, copy and unload). Some columns might not be relevant depending on the query type.
{% enddocs %}

{% docs redshift_diskusage %}
The SVV_DISKUSAGE view contains information about data allocation for the tables in a database. 
Use the SVV_DISKUSAGE system view by joining the STV_TBL_PERM and STV_BLOCKLIST tables. 
{% enddocs %}

{% docs redshift_connection_log %}
[SYS_CONNECTION_LOG](https://docs.aws.amazon.com/redshift/latest/dg/SYS_CONNECTION_LOG.html) 
logs authentication attempts and connections and disconnections. 
SYS_CONNECTION_LOG is visible to all users but only superusers have access to all rows. 
Other users can only see rows relating to their own session.
{% enddocs %}

{% docs redshift_table_info %}
[SVV_TABLE_INFO](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html) 
shows summary information for tables and materialized views in the currently connected database. 
The view filters out system tables, and shows only user-defined tables and materialized views 
that contain at least 1 row of data. You can use SVV_TABLE_INFO to diagnose and address table 
design issues that can influence query performance, including issues with compression encoding, 
distribution keys, sort style, data distribution skew, table size, and statistics. 
SVV_TABLE_INFO is visible only to superusers.
{% enddocs %}

{% docs redshift_query_metrics %}
[STV_QUERY_METRICS](https://docs.aws.amazon.com/redshift/latest/dg/r_STV_QUERY_METRICS.html) 
contains metrics information, such as the number of rows processed, CPU usage, input/output, 
and disk use, for active queries running in user-defined query queues (service classes). 
Query metrics are sampled at one second intervals. STV_QUERY_METRICS tracks and aggregates 
metrics at the query, segment, and step level. STV_QUERY_METRICS is visible to all users. 
Superusers can see all rows; regular users can see only their own data. 
Note: Some or all of the data in this table can also be found in SYS_QUERY_DETAIL.
{% enddocs %}