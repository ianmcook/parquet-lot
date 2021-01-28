import sys, json
args = json.loads(sys.argv[1].replace('\\"', '"')) if len(sys.argv) > 1 else {}

from pyspark.sql import SparkSession
# add additional imports here if needed

spark = SparkSession.builder.getOrCreate()
#
# add your code here
# the **kwargs passed to run_on_spark() are in args
#
spark.stop()
