import cloud_storage
from unittest import TestCase
from unittest import mock
import unittest

class TestCloudStorage(TestCase):
  """Tests the cloud_storage.CloudStorage class."""

  def setUp(self):
    self.cs = cloud_storage.CloudStorage(mock.MagicMock(), 'bucket', 'dataset')

  def test_getStorageFolders(self):
    """Tests the getStorageFolders() method."""

    self.cs.cs_api.objects().list().execute.return_value = {
      'prefixes': ['/dev/null/magic/', 'testdataset/table1/', '/thing/']
    }
    self.assertEqual(self.cs.getStorageFolders('/test/dir'), ['magic',
                                                              'table1',
                                                              'thing'])

  def test_uploadData(self):
    """Tests the uploadData() method."""

    cloud_storage.NamedTemporaryFile = mock.MagicMock()

    cloud_storage.NamedTemporaryFile().name = 'some file.json'
    self.cs.cs_api.objects().insert = mock.MagicMock()

    # call the method
    self.cs.uploadData('/test.json', 'some data')

    # verify
    self.cs.cs_api.objects().insert.assert_called_with(
      bucket=self.cs.bucket, media_body='some file.json', name='/test.json')


if __name__ == '__main__':
  unittest.main()
