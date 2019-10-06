
from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging
import pyspark.sql.functions as f


CORE_PATH = '/mnt1/data'

def main():

  if not os.path.exists(CORE_PATH + '/fact_summary'):
    os.makedirs(CORE_PATH + '/fact_summary')

  logger = logging.getLogger(__name__)

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  immigration_aggregate = spark.read.parquet(CORE_PATH + '/dimension_tables/d_age_and_sex_join')
  weather_and_demography_aggregate = spark.read.parquet(CORE_PATH + '/dimension_tables/d_temperature')

  combined_aggregate = immigration_aggregate.join(
    weather_and_demography_aggregate,
    immigration_aggregate.state == weather_and_demography_aggregate.state_code,
    how='left'
  ).drop(immigration_aggregate.state)

  combined_aggregate.write.mode('overwrite').partitionBy('state_code', 'state', 'arrival_month').parquet(
    CORE_PATH + '/fact_summary/f_immigration_weather_demography'
  )

  spark.stop()

if __name__ == '__main__':
  main()
