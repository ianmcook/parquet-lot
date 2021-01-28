import sys, json
args = json.loads(sys.argv[1].replace('\\"', '"')) if len(sys.argv) > 1 else {}

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

schema = StructType([
    StructField("ByteType_null_ok", ByteType(), True),
    StructField("ByteType_no_null", ByteType(), False),
    StructField("ShortType_null_ok", ShortType(), True),
    StructField("ShortType_no_null", ShortType(), False),
    StructField("IntegerType_null_ok", IntegerType(), True),
    StructField("IntegerType_no_null", IntegerType(), False),
    StructField("LongType_null_ok", LongType(), True),
    StructField("LongType_no_null", LongType(), False),
    StructField("FloatType_null_ok", FloatType(), True),
    StructField("FloatType_no_null", FloatType(), False),
    #TODO(ianmcook): continue adding types here:
    #https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    #more detail at https://spark.apache.org/docs/2.4.0/sql-reference.html
    #See nested types examples here:
    #https://docs.databricks.com/_static/notebooks/transform-complex-data-types-python.html
])

#rows contain:
# 0. bottom of range
# 1. top of range
# 2. zero/empty
# 3. null for nullable fields, zero/empty for non-nullable

#TODO(ianmcook): continue adding rows here
json = """
[
    {
      "ByteType_null_ok": -128,
      "ByteType_no_null": -128,
      "ShortType_null_ok": -32768,
      "ShortType_no_null": -32768,
      "IntegerType_null_ok": -2147483648,
      "IntegerType_no_null": -2147483648,
      "LongType_null_ok": -9223372036854775808,
      "LongType_no_null": -9223372036854775808,
      "FloatType_null_ok": -3.40282346638528860e+38,
      "FloatType_no_null": -1.40129846432481707e-45
    },
    {
      "ByteType_null_ok": 127,
      "ByteType_no_null": 127,
      "ShortType_null_ok": 32767,
      "ShortType_no_null": 32767,
      "IntegerType_null_ok": 2147483647,
      "IntegerType_no_null": 2147483647,
      "LongType_null_ok": 9223372036854775807,
      "LongType_no_null": 9223372036854775807,
      "FloatType_null_ok": 3.40282346638528860e+38,
      "FloatType_no_null": 1.40129846432481707e-45
    },
    {
      "ByteType_null_ok": 0,
      "ByteType_no_null": 0,
      "ShortType_null_ok": 0,
      "ShortType_no_null": 0,
      "IntegerType_null_ok": 0,
      "IntegerType_no_null": 0,
      "LongType_null_ok": 0,
      "LongType_no_null": 0,
      "FloatType_null_ok": 0.0,
      "FloatType_no_null": 0.0
    },
    {
      "ByteType_null_ok": null,
      "ByteType_no_null": 0,
      "ShortType_null_ok": null,
      "ShortType_no_null": 0,
      "IntegerType_null_ok": null,
      "IntegerType_no_null": 0,
      "LongType_null_ok": null,
      "LongType_no_null": 0,
      "FloatType_null_ok": null,
      "FloatType_no_null": 0.0
    }
]
"""
#TODO(ianmcook): consider adding additional rows with above-range and below-
#range values for some types (like the floating point types) but not for the
#types for which this causes JSON parsing errors

data = spark.read.schema(schema).json(sc.parallelize([json]))
# data.show() # for debugging

#TODO(ianmcook): write several copies of data with some of the other Parquet
#options toggled off/on:
# https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#configuration

comps = args.get('compression') or ['none']
for comp in comps:
    file = 'artifacts/all_types_spark_' + spark.version + '_' + comp
    data.repartition(1).write.parquet(file, compression = comp)

spark.stop()
