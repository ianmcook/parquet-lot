# spark_write_nan_inf
Use multiple versions of Spark to write small Parquet files containing floating point numeric columns with `NaN`, `Infinity`, and `-Infinity` values

## Details
- Writes Parquet files with:
  - Two columns
    - Float
    - Double
  - Three records
    - `NaN` values in the first
    - `Infinity` values in the second
    - `-Infinity` values in the third
- Writes multiple versions of each file using different compression formats
  - Uncompressed, Snappy, Gzip, and LZ4
- Writes a reference JSON file containing the source data
