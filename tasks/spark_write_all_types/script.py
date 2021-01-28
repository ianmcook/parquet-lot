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
    StructField("DoubleType_null_ok", DoubleType(), True),
    StructField("DoubleType_no_null", DoubleType(), False),
    StructField("DecimalType_38_0_null_ok", DecimalType(38, 0), True),
    StructField("DecimalType_38_0_no_null", DecimalType(38, 0), False),
    StructField("DecimalType_38_38_null_ok", DecimalType(38, 38), True),
    StructField("DecimalType_38_38_no_null", DecimalType(38, 38), False),
    StructField("StringType_null_ok", StringType(), True),
    StructField("StringType_no_null", StringType(), False)
    #TODO(ianmcook): continue adding types here:
    #https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    #more detail at https://spark.apache.org/docs/2.4.0/sql-reference.html
    #See nested types examples here:
    #https://docs.databricks.com/_static/notebooks/transform-complex-data-types-python.html
])

#rows contain:
# 0. bottom of range (for numeric types)
# 1. top of range (for numeric types)
# 2. zero/empty
# 3. negative zero/empty
# 4. null for nullable fields, zero/empty for non-nullable

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
      "FloatType_no_null": -1.40129846432481707e-45,
      "DoubleType_null_ok": -4.94065645841246544e-324,
      "DoubleType_no_null": -1.79769313486231570e+308,
      "DecimalType_38_0_null_ok": -99999999999999999999999999999999999999,
      "DecimalType_38_0_no_null": -99999999999999999999999999999999999999,
      "DecimalType_38_38_null_ok": -0.00000000000000000000000000000000000001,
      "DecimalType_38_38_no_null": -0.00000000000000000000000000000000000001,
      "StringType_null_ok": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
      "StringType_no_null": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
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
      "FloatType_no_null": 1.40129846432481707e-45,
      "DoubleType_null_ok": 4.94065645841246544e-324,
      "DoubleType_no_null": 1.79769313486231570e+308,
      "DecimalType_38_0_null_ok": 99999999999999999999999999999999999999,
      "DecimalType_38_0_no_null": 99999999999999999999999999999999999999,
      "DecimalType_38_38_null_ok": 0.00000000000000000000000000000000000001,
      "DecimalType_38_38_no_null": 0.00000000000000000000000000000000000001,
      "StringType_null_ok": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
      "StringType_no_null": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
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
      "FloatType_no_null": 0.0,
      "DoubleType_null_ok": 0.0,
      "DoubleType_no_null": 0.0,
      "DecimalType_38_0_null_ok": 0,
      "DecimalType_38_0_no_null": 0,
      "DecimalType_38_38_null_ok": 0.0,
      "DecimalType_38_38_no_null": 0.0,
      "StringType_null_ok": "",
      "StringType_no_null": ""
    },
    {
      "ByteType_null_ok": -0,
      "ByteType_no_null": -0,
      "ShortType_null_ok": -0,
      "ShortType_no_null": -0,
      "IntegerType_null_ok": -0,
      "IntegerType_no_null": -0,
      "LongType_null_ok": -0,
      "LongType_no_null": -0,
      "FloatType_null_ok": -0.0,
      "FloatType_no_null": -0.0,
      "DoubleType_null_ok": -0.0,
      "DoubleType_no_null": -0.0,
      "DecimalType_38_0_null_ok": -0,
      "DecimalType_38_0_no_null": -0,
      "DecimalType_38_38_null_ok": -0.0,
      "DecimalType_38_38_no_null": -0.0,
      "StringType_null_ok": "",
      "StringType_no_null": ""
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
      "FloatType_no_null": 0.0,
      "DoubleType_null_ok": null,
      "DoubleType_no_null": 0.0,
      "DecimalType_38_0_null_ok": null,
      "DecimalType_38_0_no_null": 0,
      "DecimalType_38_38_null_ok": null,
      "DecimalType_38_38_no_null": 0.0,
      "StringType_null_ok": null,
      "StringType_no_null": ""
    }
]
"""

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
