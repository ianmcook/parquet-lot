from polyspark import run_on_spark
from pkg_resources import parse_version

vers = ['2.0.0', '3.0.0']
comps_200 = ['none', 'snappy', 'gzip'] #, 'lzo'] # need extra libraries
comps_240 = [] #'brotli', 'lz4', 'zstd'] # need extra libraries

for ver in vers:
  comps = comps_200
  if parse_version(ver) >= parse_version('2.4.0'):
    comps += comps_240
  run_on_spark('tasks/write_all_types/script.py', ver, compression = comps)
