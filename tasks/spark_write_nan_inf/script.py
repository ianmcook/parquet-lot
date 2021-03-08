import sys, json
args = json.loads(sys.argv[1].replace('\\"', '"')) if len(sys.argv) > 1 else {}

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

schema = StructType([
    StructField("f", FloatType()),
    StructField("d", DoubleType())
])

#rows contain:
# 0. NaN
# 1. Infinity
# 2. -Infinity

json = """
[
    {"f": NaN, "d": NaN},
    {"f": Infinity, "d": Infinity},
    {"f": -Infinity, "d": -Infinity}
]
"""

data = spark.read.schema(schema).json(sc.parallelize([json]))
# data.show() # for debugging

task_name = args.get('task_name') or 'task'
comps = args.get('compression') or ['none']

for comp in comps:
    file = 'artifacts/' + task_name + '_' + spark.version + '_' + comp
    data.repartition(1).write.parquet(file, compression = comp)

spark.stop()

with open('artifacts/' + task_name + '_' + 'reference.json', 'w') as ref:
    ref.write(json)
