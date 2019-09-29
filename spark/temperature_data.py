from pyspark.sql import SparkSession
import os
import logging
CORE_PATH = '/mnt1/data'

def main():

  logger = logging.getLogger()
  logger.error('TEMPERATURE JOB RUNNING')

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  temperature_data = spark.read.csv(
    CORE_PATH + '/climate-change-earth-surface-temperature-data/GlobalLandTemperaturesByState.csv',
    header=True,
    inferSchema=True
  )

  temperature_data = temperature_data.filter(
    (temperature_data.Country == 'United States') & 
    temperature_data.dt.contains('2013')
   )

  logger.error(temperature_data.show(10))

if __name__ == '__main__':
  main()
