# self.cache_manager = CacheManager(config)
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import warnings

from ._file_io import SparkFileIO, fmakedirs, finfo, fdelete, fexists

class CacheManager:
	"""Interface to manage cache."""
	def __init__(self, spark_session: SparkSession, config: dict):
		"""
		Args:
			config (dict): Dict object containing the cache configuration for Spark engine.
		Raises:
			ValueError: If 'cache_dir' is missing or empty in the configuration.
		"""
		self._file_io = SparkFileIO(spark_session)
		self._cache_dir = config.get('cache_dir', '')
		self._cache_valid = config.get('cache_valid', 0)
		
		if self._cache_dir:
			fmakedirs(self._cache_dir)
		else:
			raise ValueError("'cache_dir' missing or empty in configuration")

	def _hash_sql(self, sql: str) -> str:
		"""Generate a hash for a given SQL query.
		
		Args:
            sql (str): SQL query string.
			
        Returns:
            str: A SHA-224 hash of the SQL query.
		"""
		return hashlib.sha224(sql.encode()).hexdigest()

	def _get_cache_path(self, cache_key: str) -> str:
		"""Get the full path of the cache file.
		
		Args:
            cache_key (str): The hash key for the cache.
            
        Returns:
            str: Full path to the cache file.
		"""
		return f"{self._cache_dir}/{cache_key}.parquet"

	def _is_cache_valid(self, cache_key: str, cache_valid_days: int) -> bool:
		"""Check if the current cache is valid.
		Args:
			cache_key (str): The hash key for the cache.
			cache_valid_days (int): Number of days the cache is considered valid.
		Returns:
			bool: True if the cache is valid, False otherwise.
		"""
		cache_file = self._get_cache_path(cache_key)
		if not fexists(cache_file):
			return False
		try:
			file_info = finfo(cache_file)
		except Exception as e:
			print(f"[CacheManager] Error getting file info for {cache_file}: {e}")
			return False
		if not file_info:
			return False
		last_modified_time = file_info['mtime']
		if cache_file.startswith('hdfs://'):
			# Assume last_modified_time is already a datetime for hdfs
			date_modified = last_modified_time
			now = datetime.now(tz=getattr(date_modified, 'tzinfo', None))
		else:
			date_modified = datetime.fromtimestamp(last_modified_time)
			now = datetime.now()
		diff_days = abs((now - date_modified).days)
		return diff_days <= cache_valid_days

	def check_cache_hit(self, query: str, cache_valid: int = None):
		cache_key = self._hash_sql(query)
		cache_path = self._get_cache_path(cache_key)
		if cache_valid is None:
			cache_valid = self._cache_valid
		
		# Only check cache if cache_valid > 0 (caching is enabled)
		if cache_valid > 0 and self._is_cache_valid(cache_key, cache_valid):
			try:
				data = self._file_io.fread(cache_path)
				# Cache is valid and data read successfully
				return False, data, cache_path
			except Exception:
				warnings.warn(f'Cache file {cache_path} was not saved properly. Requerying.')
				fdelete(cache_path)
				# Requery the data
		# Invalid cache or corrupted/unreadable cache file, treat as cache miss
		return True, None, cache_path
	
	def write_to_cache(self, data, cache_path: str):
		return self._file_io.fwrite(cache_path, data)