from io import BytesIO
from upath import UPath

class SQLFileIO:
    """
    Class for reading and writing SQL files.
    """
    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> str:
        """
        Read a SQL file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing SQL data.

        Returns:
            str: Contents of the SQL file.
        """
        return b.read().decode()
    
    @staticmethod
    def _write(upath_obj: UPath, data: str, mode: str='w', *args, **kwargs):
        """
        Write data to a text file at the specified UPath location.

        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (str): Data to write to the text file.
            mode (str): Mode to open the file, default is 'w'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            f.write(data, *args, **kwargs)