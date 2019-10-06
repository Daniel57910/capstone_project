from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging
import pyspark.sql.functions as f 

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

def create_sex_distribution_frame(all_data, female_data):

  distribution_frame = all_data.join(
    female_data.withColumnRenamed('count', 'female_count'),
    [all_data.state == female_data.state, all_data.arrival_month == female_data.arrival_month]
  ).drop('female_data.state', 'female_data.arrival_month')

  distribution_frame = distribution_frame.withColumn('female_distribution', distribution_frame.female_count / distribution_frame.count)

  return distribution_frame

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
  example_age_aggregate = example.groupBy('state', 'arrival_month').avg('age')
  example_sex_aggregate = example.groupBy('state', 'arrival_month').count()
  
  example_female_aggregate = example.filter(
    example.gender == 'F'
  ).groupBy('state', 'arrival_month').count()

  sex_distirbution_aggregate = create_sex_distribution_frame(example_sex_aggregate, example_female_aggregate)

  logger.error(example_age_aggregate.show(50))
  logger.error(example_sex_aggregate.show(50))
  logger.error(example_female_aggregate.show(50))
  logger.error(sex_distirbution_aggregate.show(50))
  
  example_female_aggregate_filter = example_female_aggregate.filter(
    example_female_aggregate.state == 'IL'
  )
  
  all_filter = example_sex_aggregate.filter(
    example_sex_aggregate.state == 'IL'
  )

  logger.error(example_female_aggregate_filter.show(50))
  logger.error(all_filter.show(50))

  logger.error(all_filter.show())

if __name__ == '__main__':
  main()