# parquet-lot
Generate Parquet files for Spark/Arrow cross-version integration testing

h/t [@sethrosen](https://twitter.com/sethrosen/status/1354612746990604295) for the name

## How it works
parquet-lot is a collection of scripts that write sets of Parquet files. Currently it contains only one script that writes Parquet files using Spark; later it will be extended to write additional types of Parquet files and to write files using Arrow. parquet-lot uses polyspark to run scripts on multiple versions of Spark. The scripts are manually triggered to run on GitHub Actions. The user can specify which versions of Spark to run them on. A zip archive of the Parquet files from each run is stored in Actions as an artifact.
