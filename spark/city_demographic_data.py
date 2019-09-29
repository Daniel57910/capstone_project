from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging
CORE_PATH = '/mnt1/data'

def main():

  logger = logging.getLogger()
  logger.error('JOB RUNNING')

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  us_cities_rdd = RDDCreator(
    'city-demographic-data',
    [CORE_PATH + '/us-cities-demographics.json'],
    spark
  )

  us_cities_rdd = us_cities_rdd.create_rdd_from_path()
  us_cities_rdd = us_cities_rdd.select(
    us_cities_rdd.fields.state_code.alias('state_code'),
    us_cities_rdd.fields.state.alias('state'),
    us_cities_rdd.fields.city.alias('city'),
    us_cities_rdd.fields.male_population.alias('male_population'),
    us_cities_rdd.fields.female_population.alias('female_population'),
    us_cities_rdd.fields.race.alias('race')
  )

  if not os.path.exists(CORE_PATH + '/dimension_tables'):
    os.makedirs(CORE_PATH + '/dimension_tables')

  us_cities_rdd.write.mode('overwrite').partitionBy('state_code', 'state', 'city').parquet(
    CORE_PATH + '/dimension_tables/d_city_demographic'
  )
 
  logger.error('END OF SPARK JOB')

  spark.stop()

if __name__ == "__main__":
  main()

  