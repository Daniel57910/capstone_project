from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging

CORE_PATH = '/mnt1/data'

def generate_immigration_rdd(spark, dataset):
  return (spark.read.format("com.github.saurfang.sas.spark").load(dataset))

def main():
  
  logger = logging.getLogger(__name__)

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  file_finder = FileFinder(CORE_PATH + '/immigration-data/', '*.sas7bdat')

  immigration_data_sets = map(
    lambda file: generate_immigration_rdd(spark, file), file_finder.return_file_names()
  )

  example = list(immigration_data_sets)[0]
  logger.error(example.head(100))

if __name__ == '__main__':
  main()