# spark-gpdb
Simple spike for querying data in Greenplum DB (GPDB) from PySpark. In this notebook you'll find an example
of how to read data in parallel from Greenplum segments.


#### Process

  1. Start a PySpark session
  2. Define a "query function" to run on each Spark worker
  3. Create an RDD of (<host>, <port>) tuples to dispatch to assign Spark workers to segments
  4. Invoke the query function from step 3 on each Spark worker using:
    - Segment host and port to connect to
    - Name of the database to connect to
    - String representation of the query to run on each segment (note, it's better to perform and filtering at this step to minimize data movement)
  5. Continue about your Spark business using the resulting RDD



