# parquet-lot
Generate Parquet files with multiple versions of Apache Spark
h/t (@sethrosen)[https://twitter.com/sethrosen/status/1354612746990604295] for the name

## How it works
parquet-lot contains scripts that write Parquet files for cross-version integration testing of Apache Spark and Apache Arrow. The scripts can on multiple versions of Spark using polyspark. The scripts can be manually triggered to run on GitHub Actions and the user can specify which versions of Spark to run them on. A zip archive of the Parquet files is stored as an artifact.
