from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
import logging

def main():

  logger = logging.getLogger(__name__)
  logger.info('JOB RUNNING')
  spark = SparkSession\
  .builder\
  .appName("sparkify_etl")\
  .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  logger.info('END OF SPARK JOB')

  spark.stop()

if __name__ == "__main__":
  main()

  