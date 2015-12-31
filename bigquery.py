"""
Classes for working with the google bigquery apiclient.
"""

from uuid import uuid4

# The substituted strings should be the dataset id and tablename, respectively.
NUM_ROWS_QUERY = 'SELECT COUNT(*) FROM %s.%s'

class Bigquery(object):

  def __init__(self, bq_api, projectId, datasetId):
    """
    Args:
      bq_api: An apiclient.discovery instance of the bigquery service.
      projectId: str, The project name.
      datasetId: str, The dataset to get tables from.
    """
    self.bq_api = bq_api
    self.projectId = projectId
    self.datasetId = datasetId

  def getAllTables(self):
    """
    Queries the dataset for all the tables and returns them as a list.
    """

    tables = []
    tables_response = self.bq_api.tables().list(
      projectId=self.projectId,
      datasetId=self.datasetId).execute()

    for entry in tables_response['tables']:
      # the table id comes in the format: 'project:dataset.tablename'
      # periods are not valid in any of those names so it's safe to use
      # them as the delimiter
      table = entry['id'].split('.')[1]
      tables.append(table)

    return tables

  def getNumRows(self, tablename):
    """Get the total number of rows for a specific table.

    Args:
      tablename: str, The table to get the number of rows from.

    Returns:
      The total number of rows as a string.
    """

    query_request = self.bq_api.jobs()
    query_data = {
        'query': NUM_ROWS_QUERY % (self.datasetId, tablename)
    }
    query_response = query_request.query(
        projectId=self.projectId,
        body=query_data).execute()

    # 'rows': [{'f': [{'v': '164656'}]}]
    return query_response['rows'][0]['f'][0]['v']

  def exportToStorage(self, tablename):
    """Starts a bigquery export job that copies table data to cloud storage.

    Args:
      tablename: str, The name of the table to export.
    """

    cloud_storage_path = 'gs://%s/%s/%s/data.json.gz' % (self.projectId,
                                                         self.datasetId,
                                                         tablename)
    job_data = {
        'jobReference': {
            'projectId': self.projectId,
            'jobId': str(uuid4())
        },
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': self.projectId,
                    'datasetId': self.datasetId,
                    'tableId': tablename,
                },
                'destinationUris': [cloud_storage_path],
                'destinationFormat': 'NEWLINE_DELIMITED_JSON',
                'compression': 'GZIP'
            }
        }
    }

    self.bq_api.jobs().insert(projectId=self.projectId, body=job_data).execute()
