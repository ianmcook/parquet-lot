# tasks
Use multiple versions of Spark to write small Parquet files containing floating point numeric columns with `NaN`, `Infinity`, and `-Infinity` values

## What each task consists of
Each subdirectory of the `tasks` directory contains three files
* `script.py`: a PySpark script controlling what data is written
* `caller.py`: a Python script that calls `script.py` multiple times in series with different parameters
* `README.md`: a Markdown file describing what the task does

## How to add a new task
1. Fork the repository
2. Make a copy of one of the subdirectories of the `tasks` directory and give it a suitable new name
3. Modify the PySpark code in `script.py` (particularly the values of `schema` and `json`; the remainder of the code is somewhat general and might not need to be changed depending on what you are trying to achieve)
4. Modify `caller.py` if needed (but it is quite general so you might not need to change it)
5. Modify `README.md` to describe your newly added task
6. Submit a pull request if you would like your new task to be merged into the upstream repository
