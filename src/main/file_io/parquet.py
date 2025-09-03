from pandas import DataFrame, read_parquet
from io import BytesIO
from upath import UPath

class ParquetFileIO:
    """
    Class for reading and writing Parquet files.
    """
    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> DataFrame:
        """
        Read a Parquet file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing Parquet data.

        Returns:
            DataFrame: Data read from the Parquet file.
        """
        return read_parquet(b, *args, **kwargs)

    @staticmethod
    def _write(upath_obj: UPath, data: DataFrame, mode: str='wb', *args, **kwargs):
        """
        Write a DataFrame to a Parquet file at the specified UPath location.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (DataFrame): DataFrame to write to the Parquet file.
            mode (str): Mode to open the file, default is 'wb'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            data.to_parquet(f, *args, **kwargs)
            # data.to_parquet(f, index=False, use_content_defined_chunking=True,*args, **kwargs)