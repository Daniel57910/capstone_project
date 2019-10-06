from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging
import pyspark.sql.functions as f 
from functools import reduce 
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.union, dfs)


CORE_PATH = '/mnt1/data'

def generate_immigration_rdd(spark, dataset):
  return (spark.read.format("com.github.saurfang.sas.spark").load(dataset))

def return_rdd_column_subset(dataset):
  
  dataset = dataset.select(
    dataset.i94yr.alias('arrival_year'),
    dataset.i94mon.alias('arrival_month'),
    dataset.arrdate.alias('arrival_date'),
    dataset.depdate.alias('departure_date'),
    dataset.i94addr.alias('state'),
    dataset.i94port.alias('city_of_arrival'),
    dataset.gender,
    dataset.biryear.alias('age')      
  )

  dataset = dataset.filter(dataset.state.isNotNull())

  return dataset

def create_sex_distribution_frame(logger, all_data, female_data):
 
  female_data = female_data.toDF('f_state', 'f_arrival_month', 'f_count') 
  distribution_frame = all_data.join(
    female_data,
    [all_data.state == female_data.f_state, all_data.arrival_month == female_data.f_arrival_month]
  ).select('state', 'arrival_month', 'count', 'f_count')

  logger.error('DISTRIBUTION FRAME =>')
  logger.error(distribution_frame.show(50))
  distribution_frame = distribution_frame.withColumn('female_distribution', distribution_frame['f_count'] / distribution_frame['count'])

  return distribution_frame

def create_female_distribution(logger, dataset):

    logger.error(dataset.show(10))
    dataset = dataset.filter(dataset.gender == 'F').groupBy('state', 'arrival_month').count()
    dataset = dataset.toDF('f_state', 'f_arrival_month', 'f_count')
    logger.error(dataset.show(10))
    return dataset

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

  immigration_data_sets = list(map(return_rdd_column_subset, immigration_data_sets))
  immigration_data = unionAll(*immigration_data_sets) 
  logger.error(immigration_data.show(40))





  # age_aggregate = map(lambda dataset: dataset.groupBy('state', 'arrival_month').avg('age'), immigration_data_sets)
  # sex_aggregate = map(lambda dataset: dataset.groupBy('state', 'arrival_month').count(), immigration_data_sets)
  

  # example = list(immigration_data_sets)[0]
  # logger.info(example.show(10))

  # female_aggregate = map(
    # lambda dataset: create_female_distribution(logger, dataset), immigration_data_sets
  # )

  # example_sex = list(sex_aggregate)[2]
  # example_all = list(immigration_data_sets)[0]
  # example_female = list(female_aggregate)[0]

  # logger.error(example_sex.show(10))
  # logger.error(example_all.show(10))
  # logger.error(example_female.show(10))


  



if __name__ == '__main__':
  main()