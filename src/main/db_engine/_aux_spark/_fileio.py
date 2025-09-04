import pandas as pd
import os
from typing import Any
from pyspark.sql import SparkSession, DataFrame

from utilities import FileIO
from ..._aux import retry_args

supported_files = ['parquet', 'csv']

def fmakedirs(fpath: str) -> None:
	return FileIO.fmakedirs(fpath)

def finfo(fpath: str) -> Any:
	return FileIO.finfo(fpath)

def fdelete(fpath: str) -> None:
	return FileIO.fdelete(fpath)

def fexists(fpath: str) -> bool:
	return FileIO.fexists(fpath)

class SparkFileIO:
    # ==================================================================
    # Additional FileIO methods for Spark
    # ==================================================================
	def __init__(self, spark_session: SparkSession):
		self.spark_session = spark_session

	def get_ftype(self, fpath: str) -> str:
		_, ftype = os.path.splitext(fpath)
		return ftype[1:]

	# @retry_args
	def fread(self, fpath: str) -> DataFrame:
		'''Reads a file from local or HDFS into a Spark Dataframe.
		Args:
			fpath (str): Local or absolute path to the file to read.
		Raises:
			NotImplementedError: If the file type given/inferred is not supported for reading
		Returns:
			DataFrame: Content of the file.
		'''
		ftype = self.get_ftype(fpath)
		if ftype in supported_files:
			read_method = getattr(self.spark_session.read, ftype)
		else:
			raise NotImplementedError(f"File type not supported for reading: {ftype}")
		return read_method(fpath)

	# @retry_args
	def fwrite(self, data: DataFrame, fpath: str) -> None:
		'''Writes data to a local or HDFS file. #TODO works on local too?
		Supported file types: csv, parquet.
		Args:
			data (DataFrame): Data to write to the file.
			fpath (str): Local or absolute path of the file to write.
		Raises:
			NotImplementedError: If the file type given/inferred is not supported for writing.
		'''
		ftype = self.get_ftype(fpath)
		if ftype in supported_files:
			write_method = getattr(data.write.mode('overwrite'), ftype)
		else:
			raise NotImplementedError(f"File type not supported for writing: {ftype}")
		return write_method(fpath)