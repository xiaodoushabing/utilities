from io import BytesIO
from upath import UPath
from typing import Optional
class TextFileIO:
    """
    Class for reading and writing text files.
    """
    @staticmethod
    def _read(b: BytesIO, encoding: Optional[str] = None, *args, **kwargs) -> str:
        """
        Read a text file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing text data.
            encoding (Optional[str]): Text encoding to use for decoding. Defaults to None.

        Returns:
            str: Contents of the text file.
        """
        # Read the bytes without passing args/kwargs to read()
        if encoding is None:
            return b.read().decode()
        else:
            return b.read().decode(encoding)
    
    @staticmethod
    def _write(upath_obj: UPath, data: str, mode: str='w', encoding: Optional[str] = None, *args, **kwargs):
        """
        Write data to a text file at the specified UPath location.

        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (str): Data to write to the text file.
            mode (str): Mode to open the file, default is 'w'.
            encoding (Optional[str]): Text encoding to use for writing. Defaults to None.
        """
        # Remove encoding from kwargs to avoid passing it to f.write()
        write_kwargs = {k: v for k, v in kwargs.items() if k != 'encoding'}
        
        if encoding is None:
            with upath_obj.fs.open(upath_obj.path, mode) as f:
                f.write(data, *args, **write_kwargs)
        else:
            with upath_obj.fs.open(upath_obj.path, mode, encoding=encoding) as f:
                f.write(data, *args, **write_kwargs)