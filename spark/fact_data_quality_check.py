from pyspark.sql import SparkSession
CORE_PATH = '/mnt1/data'

def main():

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  fact_quality_check = spark.read.parquet(CORE_PATH + '/fact_summary/f_immigration_weather_demography')

  if fact_quality_check.count() == 0 or fact_quality_check.count() is None:
    raise Exception('Loading fact summary failed')

