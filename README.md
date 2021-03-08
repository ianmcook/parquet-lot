# parquet-lot
Generate Parquet files for Spark/Arrow cross-version integration testing

h/t [@sethrosen](https://twitter.com/sethrosen/status/1354612746990604295) for the name

## How it works
parquet-lot is a collection of scripts that write sets of Parquet files. Currently it contains only two scripts that write Parquet files using Spark; later it will be extended to write additional types of Parquet files and perhaps to write files using Arrow. parquet-lot uses [polyspark](https://github.com/ursa-labs/polyspark) to run scripts on multiple versions of Spark. The scripts are manually triggered to run on GitHub Actions. The user can specify which versions of Spark to run them on. A zip archive of the Parquet files and a JSON reference file from each run is stored in Actions as an artifact.

## How to use it
1. Fork the repository
2. In your fork, go to Actions
3. Enable workflows in your fork
4. Select a workflow (urrently there is only one, named **Run Tasks on Spark**)
5. Click to open the **Run workflow** meny
6. Enter the name of a task directory (currently there are only two named `spark_write_all_simple_types` or `spark_write_nan_inf`)
7. Enter a JSON array of Spark versions (no lower than version 2.0.0)
8. Click the **Run workflow** button
9. Wait for the run to finish
10. Click the finished run
11. Under **Artifacts**, click to download the zip archive of files
