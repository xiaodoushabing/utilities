from io import BytesIO
import warnings
from upath import UPath
from upath import UPath
from typing import Union, Literal

from .arrow import ArrowFileIO
from .csv import CSVFileIO
from .feather import FeatherFileIO
from .json import JsonFileIO
from .parquet import ParquetFileIO
from .pickle import PickleFileIO
from .text import TextFileIO
from .yaml import YamlFileIO

fileio_mapping = {
    "csv": CSVFileIO,
    "txt": TextFileIO,
    "text": TextFileIO,
    "json": JsonFileIO,
    "yaml": YamlFileIO,
    "yml": YamlFileIO,
    "arrow": ArrowFileIO,
    "feather": FeatherFileIO,
    "parquet": ParquetFileIO,
    "pickle": PickleFileIO,
    "pkl": PickleFileIO,
}

class BaseFileIO:
    """
    Base class for file I/O operations.
    Provides a unified interface for reading and writing files.
    """
    def __init__(self, upath_obj: UPath, *args, **kwargs):
        """
        Initialize the BaseFileIO with a UPath object.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
        """
        self.upath = upath_obj

    def _finfo(self, *args, **kwargs) -> dict:
        """
        Get file information.
        """
        try:
            return self.upath.fs.finfo(self.upath.path, *args, **kwargs)
        except OSError as e:
            warnings.warn(f"{self.upath.path} does not exist or is not accessible.")
            raise e
        
    def _fread(self, *args, **kwargs) -> object:
        """
        Read the file content.
        
        Returns:
            object: Parsed file content.
        """
        with self.upath.fs.open(self.upath.path, 'rb') as f:
            data: BytesIO = BytesIO(f.read(*args, **kwargs))
        
        file_io_cls = fileio_mapping[self.upath.suffix[1:]]
        return file_io_cls._read(data)
    
    def _copy(self, dest_path: str, *args, **kwargs) -> None:
        """
        Copy the file to a new location.
        
        Args:
            dest_path (str): Destination path for the copied file.
        """
        try:
            dest_upath: UPath = UPath(dest_path)
            with self.upath.fs.open(self.upath.path, 'rb') as src_file, dest_upath.fs.open(dest_upath.path, 'wb') as dest_file:
                dest_file.write(src_file.read(*args, **kwargs))
            # return self.upath.fs.copy(self.upath.path, dest_upath.path, *args, **kwargs
        except OSError as e:
            warnings.warn(f"Failed to copy {self.upath.path} to {dest_path}: {e}")
            raise e

    def _fwrite(self, data: object, mode: str="wb", *args, **kwargs) -> None:
        """
        Write data to the file.
        
        Args:
            data (object): Data to write to the file.
            mode (Literal['wb', 'w']): Mode to open the file, either 'wb' for binary or 'w' for text.
        """
        file_io_cls = fileio_mapping[self.upath.suffix[1:]]
        return file_io_cls._write(self.upath, data, mode, *args, **kwargs)
    
    def _fmakedirs(self, dirpath: str, exist_ok: bool = True, *args, **kwargs) -> None:
        """
        Create directories for the file path recursively.
        
        Args:
            dirpath (str): Directory path to create.
            exist_ok (bool): If True, do not raise an error if the directory already exists.

        Raises:
            OSError: If the directory cannot be created.
        """
        try:
            self.upath.fs.makedirs(dirpath, exist_ok=exist_ok, *args, **kwargs)
        except OSError as e:
            warnings.warn(f"Failed to create directories for {dirpath}: {e}")
            raise e

    def _fdelete(self, filepath: str, *args, **kwargs) -> None:
        """
        Delete the specified file or directory.

        Args:
            filepath (str): Path to the file or directory to delete.
            
        Raises:
            OSError: If the file or directory cannot be deleted.
        """
        try:
            self.upath.fs.delete(filepath, *args, **kwargs)
        except OSError as e:
            warnings.warn(f"Failed to delete {filepath}: {e}")
            raise e