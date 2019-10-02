from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging

CORE_PATH = '/mnt1/data'

def generate_immigration_rdd(spark, dataset):
  return (spark.read.format("com.github.saurfang.sas.spark").load(dataset))

def return_rdd_column_subset(dataset):
  return (
    dataset.select(
      dataset.i94yr.alias('arrival_year'),
      dataset.i94mon.alias('arrival_month'),
      dataset.arrdate.alias('arrival_date'),
      dataset.depdate.alias('departure_date'),
      dataset.i94addr.alias('state'),
      dataset.i94port.alias('city_of_arrival'),
      dataset.gender,
      dataset.biryear.alias('age')      
  ))

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

  immigration_data_sets = map(return_rdd_column_subset, immigration_data_sets)
  
  example = list(immigration_data_sets)[0]
  logger.error(example.head(100))

if __name__ == '__main__':
  main()