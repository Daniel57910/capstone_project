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
 
  distribution_frame = all_data.join(
    female_data,
    [all_data.state == female_data.f_state, all_data.arrival_month == female_data.f_arrival_month]
  ).select('state', 'arrival_month', 'count', 'f_count')

  distribution_frame = distribution_frame.withColumn('female_distribution', distribution_frame['f_count'] / distribution_frame['count'])

  return distribution_frame

def create_female_distribution(logger, dataset):

    dataset = dataset.filter(dataset.gender == 'F').groupBy('state', 'arrival_month').count()
    dataset = dataset.toDF('f_state', 'f_arrival_month', 'f_count')
    return dataset

def main():
  
  logger = logging.getLogger(__name__)

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  file_finder = FileFinder(CORE_PATH + '/immigration-data/', '*.sas7bdat')

  immigration_data_set = map(
    lambda file: generate_immigration_rdd(spark, file), file_finder.return_file_names()
  )

  immigration_data_set = list(map(return_rdd_column_subset, immigration_data_set))
  immigration_data = unionAll(*immigration_data_set) 

  age_aggregate = immigration_data.groupBy('state', 'arrival_month').avg('age')
  sex_aggregate = immigration_data.groupBy('state', 'arrival_month').count()
  female_aggregate = create_female_distribution(logger, immigration_data)
  sex_distribution = create_sex_distribution_frame(logger, sex_aggregate, female_aggregate)
  age_aggregate = age_aggregate.toDF('age_state', 'age_arrival_month', 'age')  

  age_and_sex_join = sex_distribution.join(
    age_aggregate,
    [sex_distribution.state == age_aggregate.age_state, sex_distribution.arrival_month == age_aggregate.age_arrival_month]
  ).drop('age_state', 'age_arrival_month')

  age_aggregate = age_aggregate.toDF('state', 'arrival_month', 'age')
  sex_aggregate = sex_aggregate.toDF('state', 'arrival_month', 'age')
  female_aggregate = female_aggregate.toDF('state', 'arrival_month', 'age')

  immigration_data.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(CORE_PATH + '/dimension_tables/d_immigration')
  age_aggregate.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(CORE_PATH + '/dimension_tables/d_age_aggregate')
  sex_aggregate.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(CORE_PATH + '/dimension_tables/d_sex_aggregate')
  female_aggregate.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(CORE_PATH + '/dimension_tables/d_female_aggregate')
  sex_distribution.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(CORE_PATH + '/dimension_tables/d_sex_distribution')
  age_and_sex_join.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(CORE_PATH + '/dimension_tables/d_age_and_sex_join')


if __name__ == '__main__':
  main()