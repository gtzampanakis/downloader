"""
Download URLs using a compressed disk cache and a random throttling interval.

Each Downloader maintains an sqlite3-based disk cache that utilizes zlib
compression. Network requests are only made if the cached version of the
resource has an age larger or equal to the stale_after value provided by the
programmer.

Between network requests a throttling interval needs to elapse. This throttling
interval is randomly chosen, but lies within the throttle_bounds defined by the
programmer.

HTML resources can be parsed using lxml and in this case an lxml ElementTree is
returned instead of a file object, with the links rewritten to be absolute in
order to facilitate following them. The parsing is done leniently in order to
not fail when invalid HTML is encountered.

The programmer can also supply a function that decides whether the server has
banned the client (possibly by examining the returned resource). In this case
an exception will be raised.

Downloader's features make it ideal for writing scrapers, as it can keep its
network footprint small (due to the cache) and irregular (due to the random
throttling interval).
"""
import urllib2, sqlite3, zlib, cStringIO, logging, datetime, random, time


LOGGER = logging.getLogger(__name__)

def date_to_sqlite_str(d):
	if d is None:
		return None
	return '-'.join(
			('0' if len(str(f)) == 1 else '') + str(f) for f in [d.year, d.month, d.day]
	)


class BannedException(Exception):
	pass

class HTTPCodeNotOKError(Exception):
	def __init__(self, url, code):
		Exception.__init__(self, str(url) + ' ' + str(code))
		self.code = code
		self.url = url

class Downloader:

	def __init__(self, path, throttle_bounds, headers = None):
		"""
		path -- The path to the resource cache. If the path exists it will be
		opened. This way the cache can be persistent across sessions.

		throttle_bounds -- A sequence of two numbers. The throttling interval
		(in seconds) will be a random value uniformly distributed between those
		two numbers. A new throttling interval is chosen before each network
		request.

		headers -- A dictionary of headers to be used for network HTTP
		requests. If no User-Agent header is passed via this argument then network request
		will send a Firefox 32.0 User-Agent.
		"""
		self.path = path
		self.throttle_bounds = throttle_bounds
		self.headers = headers
		self.last_download = None
		self._set_next_throttling_period()
		with self._get_conn() as conn:
			conn.execute('''
				create table if not exists
				cache
				(
					url text primary key,
					date text not null,
					content blob not null
				)
			'''
			)

	def does_show_ban(self, element):
		"""
		element -- A lxml root ElementTree.Element

		Intended to be overridden in subclasses. The default implementation
		always returns False.

		This method is only called when parse_as_html is True. The subclass can
		use this method to process this element and decide whether the server
		has banned the client.

		The motivation for this functionality is this: Some HTTP servers, when
		they deduce that the client is a robot, will return a "200 OK" status
		code but in the body of the response they will place a message
		indicating that the client has been banned. This same body will be
		returned regardless of the URL requested. If the functionality provided
		by this method did not exist then the Downloader would keep downloading
		resources and storing them in the cache, not realizing that the
		resources are useless since the bodies do not contain any useful
		information.

		If this method returns True then an Exception is raised by the
		open_url method, with a message indicating the URL that was
		being processed at the time. This means that the useless body will not
		be stored in the cache.
		"""
		return False

	def _set_next_throttling_period(self):
		self.next_throttling_period = random.uniform(*self.throttle_bounds)

	def _get_conn(self):
		conn = sqlite3.connect(self.path)
		conn.text_factory = str
		return conn

	def _download(self, url):
		if self.last_download is not None:
			LOGGER.info('Waiting until throttling period (%.3f seconds) has passed for next download...', self.next_throttling_period)
			while True:
				since_last_download = (time.time() - self.last_download)
				if self.next_throttling_period < since_last_download:
					break
				else:
					time.sleep(.1)
		LOGGER.info('Downloading url %s', url)
# It's important to set the last_download before actually starting the
# download, otherwise if an exception is thrown while downloading the next call
# will proceed to download right away, without respecting the throttling
# (probably resulting in bans).
		self.last_download = time.time()
		self._set_next_throttling_period()

		headers = {
			'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0'
		}
		headers.update(self.headers or { })
		request = urllib2.Request(
				url,
				headers = headers,
		)

		result = urllib2.urlopen(request)

		if result.getcode() != 200:
			raise HTTPCodeNotOKError(url, result.code)
		return result



	def open_url(self, url, stale_after, parse_as_html = True, **kwargs):
		"""
		Download or retrieve from cache.

		url -- The URL to be downloaded, as a string.

		stale_after -- A network request for the url will be performed if the
		cached copy does not exist or if it exists but its age (in days) is
		larger or equal to the stale_after value. A non-positive value will
		force re-download.

		parse_as_html -- Parse the resource downloaded as HTML. This uses the
		lxml.html package to parse the resource leniently, thus it will not
		fail even for reasonably invalid HTML. This argument also decides the
		return type of this method; if True, then the return type is an
		ElementTree.Element root object; if False, the content of the resource
		is returned as a bytestring.

		Exceptions raised:

		BannedException -- If does_show_ban returns True.

		HTTPCodeNotOKError -- If the returned HTTP status code is not equal to 200.

		"""
		LOGGER.debug('open_url() received url: %s', url)
		today = datetime.date.today()
		threshold_date = today - datetime.timedelta(stale_after)
		downloaded = False

		with self._get_conn() as conn:
			rs = conn.execute('''
				select content
				from cache
				where url = ?
				and date > ?
				''',
				(url, date_to_sqlite_str(threshold_date))
			)

		row = rs.fetchone()

		retry_run = kwargs.get('retry_run', False)
		assert (not retry_run) or (retry_run and row is None)
		if row is None:
			result = self._download(url)
			downloaded = True
		else:
			result = cStringIO.StringIO(zlib.decompress(row[0]))

		if parse_as_html:
			import lxml.html
			tree = lxml.html.parse(result)
			tree.getroot().url = url
			appears_to_be_banned = False
			if self.does_show_ban(tree.getroot()):
				appears_to_be_banned = True
				if downloaded:
					message = ("Function {f} claims we have been banned, "
								"it was called with an element parsed from url "
								"(downloaded, not from cache): {u}"
								.format(f = self.does_show_ban, u = url))
					LOGGER.error(message)
				LOGGER.info('Deleting url %s from the cache (if it exists) '
							'because it triggered ban page cache poisoning exception', url)
				with self._get_conn() as conn:
					conn.execute('delete from cache where url = ?', [str(url)])
				if downloaded:
					raise BannedException(message)
				else:
					return self.open_url(url, stale_after, retry_run = True)
		else:
			tree = result.read()

		if downloaded:
# make_links_absolute should only be called when the document has a base_url
# attribute, which it has not when it has been loaded from the database. So,
# this "if" is needed:
			if parse_as_html:
				tree.getroot().make_links_absolute(tree.getroot().base_url)
				to_store = lxml.html.tostring(
								tree,
								pretty_print = True,
								encoding = 'utf-8'
				)
			else:
				to_store = tree
			to_store = zlib.compress(to_store, 8)

			logging.debug('Storing url %s to cache', url)
			with self._get_conn() as conn:
				conn.execute('''
					insert or replace 
					into cache
					(url, date, content)
					values
					(?, ?, ?)
					''',
					(
						str(url),
						date_to_sqlite_str(today),
						to_store
					)

				)
		return tree



if __name__ == '__main__':
	pass
