
from pyspark.sql import SparkSession
import logging
from glob import glob
import logging

CORE_PATH = '/mnt1/data'

def main():

  logger = logging.getLogger()
  
  dimension_parquet_paths = list(glob(CORE_PATH + '/dimension_tables/*/'))

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")
  
  for path in dimension_parquet_paths:
    logger.error(path)
    quality_check = spark.read.parquet(path)
    if quality_check.count() == 0 or quality_check.count() is None:
      raise Exception('Loading dimension data failed')

if __name__ == "__main__":
    main()

