	
# ETL flow using Spark script and Relational database on Cloud
```
You need to replicate the scenario taught today in the classroom. You may use cloud provider (AWS or GCP) of your choice to work.

1. Push crimes data file in S3/GCS bucket.
2. Create a python/scala project having spark-core and spark-sql dependencies locally.
3. Create an ETL pipeline to fetch crimes committed in year 2007 from the data loaded in the bucket and 
   load the data into any relational DB table with (crime, count) schema. (Relational db must be on cloud).

Your code will be executed in the local environment and will connect to cloud components or services.

BONUS (1 Absolute Mark): Deploy your code on cloud and execute it from there.

Note: To execute this scenario, scripting must be done using distributed programming paradigm (MapReduce)
```

## I have done two Implementations
```
1 - Using S3, pyspark, RDS (Mysql)

Reference file: python_script.py
```
## Second approach is followed in industry
```
2 - Using S3, Athena , RDS (Mysql)

Reference file: athena_implementation_script.py
```
