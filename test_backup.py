import backup
from unittest import TestCase
from unittest import mock
import unittest

class TestBackup(TestCase):
  """Tests the backup.Backup class."""

  def setUp(self):
    self.backup = backup.Backup(mock.MagicMock(), mock.MagicMock())

  def test_getNewAndOldTables(self):
    """Tests the getNewandOldTables() method."""

    self.backup.cs.getStorageFolders = mock.MagicMock(
      return_value=['deleteddir','table1'])
    self.backup.bq.getAllTables = mock.MagicMock(
      return_value=['table1', 'newtable'])

    new_tables = ['newtable']
    existing_tables = ['table1']

    self.assertEqual((new_tables, existing_tables),
                     self.backup.getNewAndOldTables('some path'))

  def test_backupTable(self):
    """Tests the backupTable() method."""

    self.backup.cs.dataset = 'testdataset'

    self.backup.bq.getNumRows = mock.MagicMock(return_value='123')
    self.backup.cs.uploadData = mock.MagicMock()
    self.backup.getRowsPath = mock.MagicMock(
      return_value='testdataset/table1/num_rows.txt')

    # call the method
    self.backup.backupTable('table1')

    # verify
    self.backup.bq.getNumRows.assert_called_with('table1')
    self.backup.cs.uploadData.assert_called_with(
      'testdataset/table1/num_rows.txt', '123')
    self.assertEqual(self.backup.getRowsPath.call_count, 1)

  def test_updateExistingTables(self):
    """Tests the updateExistingTables() method."""

    # test a non matching number of rows
    self.backup.cs.getFileAsString = mock.MagicMock(return_value='1234')
    self.backup.bq.getNumRows = mock.MagicMock(return_value='12')
    self.backup.backupTable = mock.MagicMock()

    self.backup.updateExistingTables(['table1'])
    self.assertEqual(self.backup.cs.getFileAsString.call_count, 1)
    self.assertEqual(self.backup.bq.getNumRows.call_count, 1)
    self.assertEqual(self.backup.backupTable.call_count, 1)

    # test a matching number of rows
    self.backup.cs.getFileAsString = mock.MagicMock(return_value='123')
    self.backup.bq.getNumRows = mock.MagicMock(return_value='123')
    self.backup.backupTable = mock.MagicMock()

    self.backup.updateExistingTables(['table1'])
    self.assertEqual(self.backup.cs.getFileAsString.call_count, 1)
    self.assertEqual(self.backup.bq.getNumRows.call_count, 1)
    self.assertEqual(self.backup.backupTable.call_count, 0)

  def test_backup(self):
    """Tests the backup() method."""

    new_tables = ['some', 'new', 'tables']
    existing_tables = ['existing', 'tables']

    # test with all new tables
    self.backup.getNewAndOldTables = mock.MagicMock(
      return_value=(new_tables, []))
    self.backup.backupTable = mock.MagicMock()
    self.backup.updateExistingTables = mock.MagicMock()
    # call the method
    self.backup.backup()
    # verify
    self.backup.backupTable.assert_has_calls(
      [mock.call(table) for table in new_tables])
    self.assertEqual(self.backup.updateExistingTables.call_count, 0)

    # test with all existing tables
    self.backup.getNewAndOldTables = mock.MagicMock(
      return_value=([], existing_tables))
    self.backup.backupTable = mock.MagicMock()
    self.backup.updateExistingTables = mock.MagicMock()
    # call the method
    self.backup.backup()
    # verify
    self.assertEqual(self.backup.backupTable.call_count, 0)
    self.backup.updateExistingTables.assert_called_with(existing_tables)

    # test with both tables
    self.backup.getNewAndOldTables = mock.MagicMock(
      return_value=(new_tables, existing_tables))
    self.backup.backupTable = mock.MagicMock()
    self.backup.updateExistingTables = mock.MagicMock()
    # call the method
    self.backup.backup()
    # verify
    self.backup.backupTable.assert_has_calls(
      [mock.call(table) for table in new_tables])
    self.backup.updateExistingTables.assert_called_with(existing_tables)


if __name__ == '__main__':
  unittest.main()
