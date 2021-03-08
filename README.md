# parquet-lot
Generate Parquet files for Spark/Arrow cross-version integration testing

h/t [@sethrosen](https://twitter.com/sethrosen/status/1354612746990604295) for the name

## How it works
parquet-lot is a collection of tasks that write sets of Parquet files. Currently it contains only two tasks that write Parquet files using Spark; later it will be extended to write additional examples of Parquet files and perhaps to write files using Arrow. parquet-lot uses [polyspark](https://github.com/ursa-labs/polyspark) to run tasks on multiple versions of Spark. The tasks are manually triggered to run on GitHub Actions. The user can specify which versions of Spark to run them on. A zip archive of the Parquet files and a JSON reference file from each run is stored in Actions as an artifact.

## How to run tasks
1. Fork the repository
2. In your fork, go to **Actions** and enable workflows
3. Select a workflow (currently there is only one, named **Run Tasks on Spark**)
4. Click to open the **Run workflow** menu
5. Enter the name of a task directory (currently there are only two named `spark_write_all_simple_types` or `spark_write_nan_inf`)
6. Enter a JSON array of Spark versions (no lower than version 2.0.0)
7. Click the **Run workflow** button
8. Wait for the run to finish
9. Click the finished run
10. Under **Artifacts**, click to download the zip archive of files

## How to create your own tasks
See the `README.md` file in the [`tasks` directory](https://github.com/ursa-labs/parquet-lot/tree/master/tasks)
