'''
class to generate a schema from one file
create_rdd_from_path returns a spark RDD with that schema
'''
class RDDCreator():
  
  def __init__(self, name, file_names, spark):
    self.name = name
    self.file_names = file_names
    self.spark = spark
    self.schema = self._read_schema_from_file()
    self.RDD = None

  def create_rdd_from_path(self):
    return self.spark.read.json(
      self.file_names,
      multiLine=True,
      schema=self.schema
    )

  def _read_schema_from_file(self):
    return self.spark.read.json(
      path=self.file_names[0],
      multiLine=True
    ).schema




