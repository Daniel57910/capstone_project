from pyspark.sql import SparkSession
import os
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import logging
CORE_PATH = '/mnt1/data'

temperature_columns = [
  'dt',
  'AverageTemperature',
  'State',
  'state_code'
]

def main():


  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  us_cities_rdd = spark.read.json(CORE_PATH + '/us-cities-demographics.json')

  us_cities_rdd = us_cities_rdd.select(
    us_cities_rdd.fields.state_code.alias('state_code'),
    us_cities_rdd.fields.state.alias('state'),
    us_cities_rdd.fields.city.alias('city'),
    us_cities_rdd.fields.male_population.alias('male_population'),
    us_cities_rdd.fields.female_population.alias('female_population'),
    us_cities_rdd.fields.race.alias('race')
  )

  temperature_data = spark.read.csv(
    CORE_PATH + '/climate-change-earth-surface-temperature-data/GlobalLandTemperaturesByState.csv',
    header=True,
    inferSchema=True
  )

  temperature_data = temperature_data.filter(
    (temperature_data.Country == 'United States') & temperature_data.dt.contains('2013')
  )

  temperature_data = temperature_data.join(
    us_cities_rdd, 
    temperature_data.State == us_cities_rdd.state,
    how='left'
  ).drop(us_cities_rdd.state).select(temperature_columns)
  
  if not os.path.exists(CORE_PATH + '/dimension_tables'):
    os.makedirs(CORE_PATH + '/dimension_tables')

  us_cities_rdd.write.mode('overwrite').partitionBy('state_code', 'state', 'city').parquet(
    CORE_PATH + '/dimension_tables/d_city_demographic'
  )

  temperature_data.write.mode('overwrite').partitionBy('state_code', 'state').parquet(
    CORE_PATH + '/dimension_tables/d_temperature'
  )

  spark.stop()

if __name__ == "__main__":
  main()

  