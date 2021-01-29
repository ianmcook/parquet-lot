from polyspark import run_on_spark
from pkg_resources import parse_version
from os.path import basename, dirname

task_name=basename(dirname(__file__))
script='tasks/' + task_name + '/script.py'

comps_200 = ['none', 'snappy', 'gzip'] #, 'lzo'] # need extra libraries
comps_240 = [] #'brotli', 'lz4', 'zstd'] # need extra libraries

def run(vers=['2.0.0', '3.0.0']):
    for ver in vers:
      comps = comps_200
      if parse_version(ver) >= parse_version('2.4.0'):
        comps += comps_240
      run_on_spark(script, ver, task_name=task_name, compression=comps)
