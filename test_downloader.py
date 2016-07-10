import unittest, mock, os, StringIO
import downloader

URL = 'http://www.example.com'

class URLOpenResult:

	def __init__(self):
		self.file_obj = StringIO.StringIO('<html><body>foo</body></html>')
		self.file_obj.geturl = lambda: URL

	def get_file_obj(self):
		return self.file_obj

class URLOpenSuccessResult(URLOpenResult):
	def get_code(self):
		return 200

class URLOpenErrorResult(URLOpenResult):
	def get_code(self):
		return 201

class Tester(unittest.TestCase):

	def remove_db(self):
		if os.path.exists(self.db_path):
			os.remove(self.db_path)
	
	def setUp(self):
		self.db_path = os.path.expanduser('~/.tmp_downloader.sqlite')
		self.remove_db()
		self.downloader = downloader.Downloader(self.db_path, [0.1, 0.2])

	@mock.patch('downloader._urlopen', 
				new = lambda url, *args: URLOpenSuccessResult())
	def test_success(self):
		self.downloader.open_url(URL, 10)
# This should hit the cache:
		self.downloader.open_url(URL, 10)

	@mock.patch('downloader._urlopen', 
				new = lambda url, *args: URLOpenErrorResult())
	def test_error(self):
		self.assertRaises(
			downloader.HTTPCodeNotOKError,
			self.downloader.open_url,
			URL,
			10
		)

	def tearDown(self):
		self.downloader = None
		self.remove_db()

if __name__ == '__main__':
	unittest.main()
