"""
This module backs up Bigquery data into Cloud Storage.

For new tables in BQ that aren't in cloud storage yet, it will create a new
directory named after the table, which will contain two files: num_rows.txt and
data.json.gz

num_rows.txt contains the number of rows present in the table and data.json.gz
is the backed up table data.

For existing tables, the number of rows is compared between bigquery and the
num_rows.txt file in cloud storage. If they match, the table has not been
modified and no action is taken. If they do not match, the bigquery data is
exported to cloud storage and the num_rows.txt file is updated. Since bigquery
is an append-only database, it's safe to use the number of rows as an indicator
of data changes. New tables are automatically backed up to cloud storage.

We assume a cloud storage directory structure like this:
/projectname/dataset/table/

We assume that the /projectname/dataset/ folder has been created manually,
but it will create new table directories as needed.

Currently, tables that are deleted from BQ are simply orphaned in cloud storage.
"""

from apiclient import discovery
from auth_utils import authorize_service_account
from bigquery import Bigquery
from cloud_storage import CloudStorage
from sys import argv

class Backup(object):

  def __init__(self, cs, bq):
    """
    Args:
      cs: cloud_storage.CloudStorage instance.
      bq: bigquery.Bigquery instance.
    """
    self.cs = cs
    self.bq = bq

  def getRowsPath(self, tablename):
    return self.cs.dataset+'/'+tablename+'/'+'num_rows.txt'

  def getNewAndOldTables(self, path):
    """
    Gets the list of tables in cloud storage and bigquery, and returns the
    tables that exist in bigquery but not cloud storage, and the tables that
    exist in both systems, respectively.

    Args:
      path: str, The path to the dataset folder in cloud storage which contains
        the table subdirectories.

    Returns:
      A tuple of (list, list)
    """
    cs_tables = set(self.cs.getStorageFolders(path))
    bq_tables = set(self.bq.getAllTables())

    new_tables = bq_tables.difference(cs_tables)
    existing_tables = bq_tables - new_tables

    return list(new_tables), list(existing_tables)

  def backupTable(self, tablename):
    """
    Backs up a table in cloud storage with the data from Bigquery. Adds a file
    with the total number of rows in cloud storage, and exports the table data
    from Bigquery.

    Args:
      tablename: str, The name of the table to export.
    """
    # Update the number of rows.
    num_rows = self.bq.getNumRows(tablename)
    self.cs.uploadData(self.getRowsPath(tablename), num_rows)

    # Update the data.
    self.bq.exportToStorage(tablename)

  def updateExistingTables(self, tables):
    """
    For each table, compare the number of rows in bigquery to the number of rows
    stored previously in cloud storage - backup up the table if there's a
    mismatch, otherwise skip it.

    Args:
      tables: list, A list of tables names.
    """
    for tablename in tables:

      print('processing existing table:', tablename)

      cs_rows = self.cs.getFileAsString( self.getRowsPath(tablename) )
      bq_rows = self.bq.getNumRows(tablename)

      # data hasn't changed, skip
      if cs_rows == bq_rows:
        continue

      print('updating table', tablename)
      self.backupTable(tablename)

  def backup(self):
    """
    Creates cloud storage entries for any new bigquery tables, and updates
    existing ones.
    """

    new_tables, existing_tables = self.getNewAndOldTables(self.cs.dataset + '/')

    print('new tables:', new_tables)
    print('existing tables:', existing_tables)

    if new_tables:
      for table in new_tables:
        print('backing up new table:', table)
        self.backupTable(table)

    if existing_tables:
      self.updateExistingTables(existing_tables)


if __name__ == "__main__":

  projectID = argv[1]
  datasetID = argv[2]

  bq_api = discovery.build('bigquery', 'v2', http=authorize_service_account(
    scope='https://www.googleapis.com/auth/bigquery')
  )
  cs_api = discovery.build('storage', 'v1', http=authorize_service_account(
    scope='https://www.googleapis.com/auth/devstorage.read_write')
  )

  bq = Bigquery(bq_api, projectID, datasetID)
  cs = CloudStorage(cs_api, projectID, datasetID)

  Backup(cs, bq).backup()

