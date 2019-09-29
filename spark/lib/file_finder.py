import glob
'''
file finder class
recursively searches directory for all files that match file type
returns list of all files that match file type
'''
class FileFinder():

  def __init__(self, start_directory, file_type):
    self.start_directory = start_directory
    self.file_type = file_type

  def return_file_names(self):
    return list(
      glob.iglob(self.start_directory + "**/" + self.file_type, recursive=True)
    )



