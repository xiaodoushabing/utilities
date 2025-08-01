from pandas import DataFrame, read_feather
from io import BytesIO
from upath import UPath

class ArrowFileIO:
    """
    Class for reading and writing Arrow files.
    """
    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> DataFrame:
        """
        Read an Arrow file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing Arrow data.

        Returns:
            DataFrame: Data read from the Arrow file.
        """
        return read_feather(b, *args, **kwargs)

    @staticmethod
    def _write(upath_obj: UPath, data: DataFrame, mode: str='wb', *args, **kwargs): 
        """
        Write a DataFrame to an Arrow file at the specified UPath location.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (DataFrame): DataFrame to write to the Arrow file.
            mode (str): Mode to open the file, default is 'wb'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            data.to_feather(f, *args, **kwargs)