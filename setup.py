from distutils.core import setup

setup(
	name = 'downloader',
	version = '0.96',
	author = 'Giorgos Tzampanakis',
	author_email = 'giorgos.tzampanakis@gmail.com',
	py_modules = ['downloader'],
	license = 'MIT',
	platforms = 'Any',
	requires = [ 
		'lxml (> 3.2.1)',
	],
	description = 'Download URLs using a compressed disk cache and a random throttling interval.',
	long_description = """
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
	""",
	classifiers = ['Programming Language :: Python',
					'License :: OSI Approved :: MIT License',
					'Operating System :: OS Independent',
					'Development Status :: 4 - Beta',
					'Intended Audience :: Developers',
					'Topic :: Internet :: WWW/HTTP',
					'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
	],
)
