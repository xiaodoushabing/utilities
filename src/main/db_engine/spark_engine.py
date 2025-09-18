
from urllib.parse import quote_plus
import subprocess
import warnings
import pandas as pd

from pyspark import SparkConf, SparkContext
import pyspark.sql as pysql
from pyspark.sql import types, functions
from pyspark.sql import DataFrame as SparkDataFrame

from ._aux_spark import CacheManager, HiveDataManager, EDWDataManager
from ._aux_spark._fileio import fmakedirs

supported_sub_engines = ['hive', 'edw']
supported_get_schema = ['hive']
supported_delete = ['hive']
supported_create = ['hive']

class SparkEngine:
	"""
	SparkEngine manages Spark session configurations and credentials for connecting to various data sources.
	Args:
		engine_config (dict): Configuration dictionary containing engine settings, including:
			- 'default': Default engine configuration.
			- 'presets': Preset configurations for different scenarios.
			- 'cache': Cache-related configuration.
			- 'edw': EDW-specific configuration.
		all_credentials (dict, optional): Dictionary containing all engine credentials (edw, hive, etc.). Defaults to None.
		user (str, optional): Username for authentication. If not provided, will be fetched from credentials. Defaults to None.
		password (str, optional): Password for authentication. If not provided, will be fetched from credentials. Defaults to None.
	Raises:
		ValueError: If all_credentials is not provided or doesn't contain required sub-engine credentials.
	Attributes:
		_all_credentials (dict): Stores credentials for all sub-engines.
		_engine_config (dict): Stores the default engine configuration.
		_engine_presets (dict): Stores preset configurations for the engine.
		_cache_config (dict): Stores cache-related configurations.
		_edw_config (dict): Stores Enterprise Data Warehouse specific configurations.
		spark_session (None or SparkSession): Placeholder for Spark session, initially None.
	"""
	def __init__(
		self,
		engine_config: dict,
		all_credentials: dict = None,
		user: str = None,
		password: str = None
	):
		# Store all credentials for sub-engines
		if all_credentials is None:
			raise ValueError("all_credentials must be provided for SparkEngine to access sub-engine credentials")
		
		edw_creds = all_credentials.get("edw")
		if not edw_creds or not edw_creds.get("user") or not edw_creds.get("password"):
			raise ValueError("Credentials for 'edw' must include non-empty 'user' and 'password' for SparkEngine")
		
		
		self._all_credentials = all_credentials
		self._user_override = user
		self._password_override = password
		
		self._engine_config = dict(**engine_config['default'])
		self._engine_presets = dict(**engine_config['presets'])
		self._cache_config = dict(**engine_config['cache'])
		self._edw_config = dict(**engine_config['edw'])
		self.spark_session = None
		print("SparkEngine activated: Spark support is enabled.")	

	def _get_sub_engine_credentials(self, sub_engine: str) -> dict:
		"""
		Get credentials for a specific sub-engine.
		
		Args:
			sub_engine (str): The sub-engine name (e.g., 'edw', 'hive')
			
		Returns:
			dict: Dictionary with 'user' and 'password' keys
			
		Raises:
			ValueError: If credentials for the sub-engine are not found
		"""
		sub_engine = sub_engine.lower()
		
		if sub_engine not in self._all_credentials:
			raise ValueError(f"Credentials for sub-engine '{sub_engine}' not found in configuration")
		
		credentials = self._all_credentials[sub_engine]
		
		# First priority: Use config-specific credentials if they exist and are not empty
		config_user = credentials.get("user", "")
		config_password = credentials.get("password", "")
		
		# Use config credentials if they exist, otherwise fall back to overrides
		user = config_user if config_user else self._user_override
		password = config_password if config_password else self._password_override
		
		if not user or not password:
			raise ValueError(f"Invalid credentials for sub-engine '{sub_engine}': user='{user}', password={'***' if password else 'None'}")
		
		return {
			"user": user,
			"password": password
		}

	def query(self, query: str, sub_engine: str, cache_valid: int = None, **kwargs) -> SparkDataFrame:
		"""
		Executes a query on the given sub_engine.
		Args:
			query (str): Query to be executed
			sub_engine (str): Engine to query on (see supported engines).
			cache_valid (int): Number of days to check valid cache. Defaults to 0
		Returns:
			pyspark.sql.DataFrame: A Spark DataFrame containing the results of the query.
		"""
		self.connect()
		query_db, data, cache_path = self.cache_manager.check_cache_hit(query, cache_valid)
		if query_db:
			data_manager = self._get_data_manager(sub_engine.lower())
			data = data_manager.query(query)
			if cache_valid and cache_valid > 0:
				self.cache_manager.write_to_cache(data, cache_path)
		return data

	def execute(self, query: str, sub_engine: str, cache_valid: int = None, **kwargs) -> SparkDataFrame:
		"""
		Performs same function as query. Retained for compatibility with other engines.
		"""
		return self.query(query, sub_engine, cache_valid, **kwargs)


	def write(
		self,
		data,
		table: str,
		database: str,
		sub_engine: str,
		mode: str = 'append',
		n_partitions: int = None,
		partition_col=None,
		**kwargs
	):
		"""
		Writes data to a table with the given sub_engine.
		Args:
			data (dataframe.DataFrame): Data to be written to the table.
			table (str): Table name.
			database (str): Database name.
			sub_engine (str): Engine to write to (see supported engines).
			mode (str, optional): Writing mode. Defaults to 'append'.
			n_partitions (int, optional): Number of partitions used when writing the data. Defaults to None.
			partition_col (str, optional): Column to partition when writing the data.
		Raises:
			ValueError: If both n_partitions and partition_col are provided simultaneously.
		"""
		self.connect()
		if n_partitions is not None and partition_col is not None:
			raise ValueError('Choose to partition by either a number or a column, not both')
		data_manager = self._get_data_manager(sub_engine.lower())
		if sub_engine == 'hive':
			if n_partitions is None and partition_col is None:
				warnings.warn('Partitioning the dataset by 200 for writing to Hive')
				n_partitions = 200
			data_manager.write(data, table, database, mode, n_partitions, partition_col)
		elif sub_engine == 'edw':
			if n_partitions is None:
				warnings.warn('Partitioning the dataset by 10 for writing to EDW')
				n_partitions = 10
			data_manager.write(data, table, database, mode, n_partitions)

	def _get_connection(self):
		"""Check if the Spark session is initialized.
		Returns:
			bool: True if the Spark session exists, False otherwise.
		"""
		return self.spark_session is not None

# =======================================================================
# Additional connection-specific methods for Spark
# =======================================================================

	def _get_conf(self, override={}):
		"""Internal method. Retrieves the Spark config."""
		conf = SparkConf()
		config = {**self._engine_config, **override}
		for k, v in config.items():
			conf.set(k, v)
		return conf

	def _create_spark_session(self, conf):
		"""Internal method. Creates a Spark session."""
		try:
			# Stop any existing SparkContext to avoid conflicts
			if SparkContext._active_spark_context is not None:
				SparkContext._active_spark_context.stop()
			
			# Create SparkSession with proper Hive support
			spark_session = (
				# pysql.SparkSession(SparkContext.getOrCreate(conf=conf))
				# .builder.appName('Spark App')
				pysql.SparkSession.builder
				.appName('Spark App')
				.config(conf=conf)
				.enableHiveSupport()
				.getOrCreate()
			)
			return spark_session
		except Exception as e:
			raise RuntimeError(f"Failed to create Spark session: {e}")


	def _setup_logs(self):
		"""Creates the spark logs folder and assigns the driver log path as the event log path."""
		logs_dir = self._engine_config['spark.eventLog.dir']
		fpath = '\/'.join(logs_dir.split('/') + ['spark-driver.log'])
		fmakedirs(logs_dir)
		
		cmd = f"sed -i 's/^log4j.appender.file.file .*$/log4j.appender.file.file={fpath}/' /etc/spark/conf/log4j.properties"
		result = subprocess.run(cmd, shell=True, executable='/bin/bash', check=False, capture_output=True, text=True)  # TODO warning
		if result.returncode != 0:
			warnings.warn(f"Command failed with error: {result.stderr}")


	def instantiate_managers(self):
		"""
		Instantiate and initialize the data and cache manager objects used by the SparkEngine.
		This method creates:
			- cache_manager: Manages caching mechanisms using the Spark session and cache configuration.
			- hive_data_manager: Handles interactions with Hive data sources via the Spark session.
			- edw_data_manager: Handles interactions with EDW data sources via the Spark session.
		The instantiated managers are assigned as instance attributes:
			- self.cache_manager
			- self.hive_data_manager
			- self.edw_data_manager
		"""
		self.cache_manager = CacheManager(self.spark_session, self._cache_config)
		self.hive_data_manager = HiveDataManager(self.spark_session)
		
		# Get EDW-specific credentials for EDW data manager
		edw_credentials = self._get_sub_engine_credentials('edw')
		self.edw_data_manager = EDWDataManager(
			spark_edw_credential=edw_credentials,
			spark_edw_config=self._edw_config,
			spark_session=self.spark_session,
		)


	def connect(self, preset='s', config_override={}):
		"""
		Creates and initializes a Spark session required for Spark operations.
		This method sets up the Spark session based on a configuration preset and optional overrides.
		It also initializes related managers necessary for data handling and caching.
		Args:
			preset (str, optional): The configuration preset key to use for Spark session settings. This overrides the default configuration with predefined settings. Defaults to 's'.
			config_override (dict, optional): A dictionary of configuration parameters to override both the default and preset configurations. Defaults to an empty dictionary.
		Raises:
			RuntimeError: If the Spark session is created but the instantiation of managers fails. This indicates that subsequent operations depending on these managers may not function correctly.
		Returns:
			None
		"""
		if self.spark_session:
			return
		
		conf = self._engine_presets[preset.lower()]
		conf.update(config_override)
		conf = self._get_conf(conf)
		
		self._setup_logs()
		self.spark_session = self._create_spark_session(conf)
		try:
			self.instantiate_managers()
		except Exception as e:
			raise RuntimeError(
				f"Spark session creation failed: {e}\n"
				"Managers are not instantiated.\n"
				"Subsequent operations may not work as expected."
			)


	def disconnect(self):
		"""Disconnects from the existing Spark session."""
		if self.spark_session:
			self.spark_session.stop()
			self.spark_session = None
		else:
			print("No existing Spark session to disconnect from.")

# =======================================================================
# Additional specific methods for Spark
# =======================================================================

	def _get_data_manager(self, sub_engine: str):
		"""
		Retrieve the data manager object corresponding to the specified database sub_engine.
		Args:
			sub_engine (str): The name of the database engine (e.g., 'hive', 'edw')
		Raises:
			NotImplementedError: If the specified sub_engine is not in the list of supported engines.
			NotImplementedError: If the data manager attribute for the specified sub_engine is not found.
		Returns:
			object: The data manager instance associated with the given sub_engine.
		"""
		sub_engine = sub_engine.lower()
		if sub_engine not in supported_sub_engines:
			raise NotImplementedError(f'Sub-engine not supported for querying: {sub_engine}')
		data_manager = getattr(self, f"{sub_engine}_data_manager", None)
		if data_manager is None:
			raise NotImplementedError(f"Data manager for sub_engine '{sub_engine}' not found.")
		return data_manager


	def get_table_schema(self, table: str, database: str, sub_engine: str, **kwargs):
		"""
		Get table schema, in pySpark format, for a specific table.
		Args:
			table (str): Table name.
			database (str): Database name.
			sub_engine (str): Engine to write to (see supported engines).
		Returns:
			data_schema (pd.DataFrame): Pandas dataframe of table's schema, with column names in the same corresponding order.
		"""
		self.connect()
		sub_engine = sub_engine.lower()

		if sub_engine not in supported_get_schema:
			raise KeyError(f"get_table_schema is not supported for {sub_engine}. Supported sub_engines: {supported_get_schema}")

		else:
			table_describe = self.query(f'DESCRIBE {database}.{table}', sub_engine=sub_engine).toPandas()
			data_schema = (
				table_describe.query('~col_name.str.contains("^# |^$", regex=True)')
				.assign(count=1)
				.groupby(['col_name', 'data_type'], as_index=False, sort=None).agg('count')
				.assign(is_partition=lambda x: (x['count'] > 1))
			)
			data_schema = data_schema[['col_name', 'data_type', 'is_partition']]
		return data_schema


	@staticmethod
	def to_pandas(data, **kwargs) -> pd.DataFrame:
		"""
		Converts a Spark dataframe to a pandas dataframe.
		Args:
			data (dataframe.DataFrame): Spark dataframe to convert.
		Returns:
			pd.DataFrame: Converted pandas dataframe.
		"""
		# Normalizing the date
		__DATE_FORMAT = 'dd-MM-yyyy'
		for item in data.schema:
			if isinstance(item.dataType, types.DateType):
				data = data.withColumn(item.name, functions.to_timestamp(data[item.name], __DATE_FORMAT))
		data_df = data.toPandas()
		return data_df

	def to_spark(self, data: pd.DataFrame, schema: str = None, **kwargs):
		"""
		Converts a pandas dataframe to a Spark dataframe.
		Args:
			data (pd.DataFrame): Pandas dataframe to convert.
			schema (Union[pyspark.sql.types.DataType, str or list], optional): Schema of pyspark table to be created. Defaults to None.
		Returns:
			dataframe.DataFrame: Converted Spark dataframe.
		"""
		spark_df = self.spark_session.createDataFrame(data=data, schema=schema)
		return spark_df
	
	def create(
		self,
		table: str,
		schema: dict,
		sub_engine: str,
		**kwargs
	) -> None:
		self.connect()
		sub_engine = sub_engine.lower()
		if sub_engine not in supported_create:
			raise KeyError(f"Create operation is not supported for {sub_engine}. Supported engines: {supported_create}")

		data_manager = self._get_data_manager(sub_engine)
		data_manager.create(table, schema, **kwargs)

	def delete(
		self,
		table: str,
		sub_engine: str,
		**kwargs
	) -> None:
		"""
		Delete a table from the specified sub_engine.
		Args:
			table (str): Table name to delete.
			sub_engine (str): Engine to delete from (see supported engines).
		"""
		self.connect()
		sub_engine = sub_engine.lower()
		if sub_engine not in supported_delete:
			raise KeyError(f"Delete operation is not supported for {sub_engine}. Supported engines: {supported_delete}")
		
		data_manager = self._get_data_manager(sub_engine)
		data_manager.delete(table)
