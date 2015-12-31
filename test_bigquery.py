import bigquery
from unittest import TestCase
from unittest import mock
import unittest

class BigqueryTest(TestCase):
  """Tests the bigquery.Bigquery class."""

  def setUp(self):
    self.bq = bigquery.Bigquery(mock.MagicMock(), 'project', 'dataset')

  def test_getAllTables(self):
    """Tests the getAllTables() method."""

    self.bq.bq_api.tables().list().execute.return_value = {
      'tables': [{'id': 'project:dataset.tablename'},
                 {'id': 'thing:thing.thing'}]
    }
    self.assertEqual(['tablename', 'thing'], self.bq.getAllTables())

  def test_getNumRows(self):
    """Tests the getNumRows() method."""

    self.bq.bq_api.jobs().query = mock.MagicMock()
    self.bq.bq_api.jobs().query().execute = mock.MagicMock(
      return_value={'rows': [{'f': [{'v': '164'}]}]}
    )
    query_data = {'query': 'SELECT COUNT(*) FROM dataset.table1'}

    # call the method
    num_rows = self.bq.getNumRows('table1')

    # verify
    self.bq.bq_api.jobs().query.assert_called_with(projectId='project',
                                                   body=query_data)
    self.assertEqual(num_rows, '164')

  def test_exportToStorage(self):
    """Tests the exportToStorage() method."""

    bigquery.uuid4 = mock.MagicMock(return_value=123)

    self.bq.bq_api.jobs().insert = mock.MagicMock()

    job_data = {
        'jobReference': {
            'projectId': 'project',
            'jobId': str(123)
        },
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': 'project',
                    'datasetId': 'dataset',
                    'tableId': 'table1',
                },
                'destinationUris': ['gs://project/dataset/table1/data.json.gz'],
                'destinationFormat': 'NEWLINE_DELIMITED_JSON',
                'compression': 'GZIP'
            }
        }
    }

    # call the method
    self.bq.exportToStorage('table1')

    # verify
    self.bq.bq_api.jobs().insert.assert_called_with(projectId='project',
                                                    body=job_data)


if __name__ == '__main__':
  unittest.main()
