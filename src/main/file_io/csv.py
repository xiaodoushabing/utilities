from pandas import DataFrame, read_csv
from io import BytesIO
from upath import UPath

class CSVFileIO:
    """
    Class for reading and writing CSV files.
    """
    
    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> DataFrame:
        """
        Read a CSV file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing CSV data.

        Returns:
            DataFrame: Data read from the CSV file.
        """
        return read_csv(b, *args, **kwargs)

    @staticmethod
    def _write(upath_obj: UPath, data: DataFrame, mode: str='w', *args, **kwargs):
        """
        Write a DataFrame to a CSV file at the specified UPath location.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (DataFrame): DataFrame to write to the CSV file.
            mode (str): Mode to open the file, default is 'w'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            data.to_csv(f, index=False, *args, **kwargs)