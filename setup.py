from distutils.core import setup

setup(
	name = 'downloader',
	version = '0.98',
	author = 'Giorgos Tzampanakis',
	author_email = 'giorgos.tzampanakis@gmail.com',
	url = 'https://github.com/gtzampanakis/downloader',
	py_modules = ['downloader' , 'memoize'],
	license = 'MIT',
	platforms = 'Any',
	requires = [ 
		'lxml (> 3.2.1)',
	],
	description = 'Download URLs using a compressed disk cache and a random throttling interval.',
	long_description = open('README').read(),
	classifiers = ['Programming Language :: Python',
					'License :: OSI Approved :: MIT License',
					'Operating System :: OS Independent',
					'Development Status :: 4 - Beta',
					'Intended Audience :: Developers',
					'Topic :: Internet :: WWW/HTTP',
					'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
	],
)
