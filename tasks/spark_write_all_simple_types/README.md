# spark_write_all_simple_types
Use multiple versions of Spark to write small Parquet files representing all supported data types

## Details
- Writes Parquet files with columns representing all simple types
  - Does not write any columns with complex/nested types
- Writes nullable and non-nullable variants of each simple type
  - Nullable columns include null values
- Writes files with a very small number of rows
- Writes deterministic output (no random values)
- Values in numeric columns exercise the extrema of their ranges
- Values in string columns exercise the full range of single-byte characters
  - Including NUL and non-printable characters
  - Do not include multibyte characters
- Values do not include NaN, Infinity, or -Infinity
