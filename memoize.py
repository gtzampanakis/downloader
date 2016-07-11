import logging, os, cPickle
from collections import deque

class MemoizedFunction:
	def __init__(self, function, max_cache_size = 10 * 1000, record_stats=False, use_disk = False):
		self.function = function
		self.max_cache_size = max_cache_size
		self.cache = {}
		self.queue = deque([], maxlen = max_cache_size)
		self.record_stats = record_stats
		self.limit_hit = False
		self.use_disk = use_disk
		if record_stats:
			self.total_calls = 0
			self.total_cache_hits = 0

	def get_cache_path(self):
		path = os.path.expanduser(os.path.join('~', '.memoized_function_cache', self.function.__module__, self.function.__name__))
		return path

	def __enter__(self):
		if self.use_disk:
			path = self.get_cache_path()
			if os.path.exists(path):
				logging.info('Loading memoization cache for function %s, from path %s ...', self.function, path)
				with open(path, 'rb') as cache_file:
					self.cache = cPickle.load(cache_file)
		return self
	
	def __exit__(self, exc_type, exc_value, traceback):
		if self.use_disk:
			path = self.get_cache_path()
			dirname = os.path.dirname(path)
			if not os.path.exists(dirname):
				os.makedirs(dirname)
			logging.info('Writing memoization cache for function %s to path %s ...', self.function, path)
			with open(path, 'wb') as cache_file:
				cPickle.dump(self.cache, cache_file, cPickle.HIGHEST_PROTOCOL)

	def __call__(self, *args, **kwargs):
		if self.record_stats:
			logging.debug('Hit rate for function %s: %s/%s, %.3f', 
				self.function, 
				self.total_cache_hits,
				self.total_calls,
				float(self.total_cache_hits) / self.total_calls if self.total_calls != 0 else float('nan')
			)
		args_key = (args, tuple(sorted(kwargs)))
		if self.record_stats:
				self.total_calls += 1
		if args_key in self.cache:
				if self.record_stats:
						self.total_cache_hits += 1
				return self.cache[args_key]
		newly_computed_result = self.function(*args, **kwargs)
		if not self.limit_hit:
			if len(self.queue) == self.max_cache_size:
				self.limit_hit = True
		if self.limit_hit:
			args_key_to_remove = self.queue.popleft()
			del self.cache[args_key_to_remove]
		self.queue.append(args_key)
		self.cache[args_key] = newly_computed_result
		return newly_computed_result

if __name__ == '__main__':
	logging.basicConfig(level = logging.DEBUG)
	@MemoizedFunction
	def f(a, **kwargs):
		return 1
	print f(1, k1 = 2)
	print f(3, k2 = 4)
	print f.cache

