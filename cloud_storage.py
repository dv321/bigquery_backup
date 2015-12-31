"""
Classes for working with the google cloud storage apiclient.
"""

from tempfile import NamedTemporaryFile

class CloudStorage(object):

  def __init__(self, cs_api, bucket, dataset):
    """
    Args:
      cs_api: An apiclient.discovery instance of the cloud storage service.
      bucket: str, The name of the bucket to work with.
      dataset: str, The name of the dataset to work with. Used as the toplevel
        directory when constructing table folders.
    """
    self.cs_api = cs_api
    self.bucket = bucket
    self.dataset = dataset

  def getStorageFolders(self, directory):
    """
    Gets all the subdirectories in a given directory.

    Args:
      directory: str, The directory whose subdirectories we list.

    Returns:
      folders: list, A list of the subdirectories.
    """
    if not directory.endswith('/'):
      directory += '/'

    # add the delimiter to ensure we only get folders
    cs_response = self.cs_api.objects().list(bucket=self.bucket,
                                             prefix=directory,
                                             delimiter='/').execute()
    folders = []
    for folder in cs_response['prefixes']:
      # get the -2 index because there's a trailing slash, so the last element
      # will be ''
      folders.append(folder.split('/')[-2])
    return folders

  def uploadData(self, filepath, data):
    """
    Uploads some data to a specified cloud storage location. Uses tempfile to
    create a named temporary file to store the data in, since the cloud storage
    api requires a filename which it reads the data from.

    Args:
      filepath: str, The location of the cloud storage file we write to.
      data: str, The data to be written.
    """
    # add a period to the last entry that split returns
    extension = '.' + filepath.split('.')[-1]

    tmp_file = NamedTemporaryFile(mode='w', suffix=extension)
    tmp_file.write(data)
    tmp_file.seek(0)
    self.cs_api.objects().insert(bucket=self.bucket,
                                 media_body=tmp_file.name,
                                 name=filepath).execute()

  def getFileAsString(self, filepath):
    """
    Returns the stringified data from a specified cloud storage location.
    """
    file_data = self.cs_api.objects().get_media(bucket=self.bucket,
                                                object=filepath).execute()
    return str(file_data, 'utf-8')
