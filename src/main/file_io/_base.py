from io import BytesIO
import warnings
from upath import UPath
from typing import Union, Literal, Optional

from .arrow import ArrowFileIO
from .csv import CSVFileIO
from .feather import FeatherFileIO
from .json import JsonFileIO
from .parquet import ParquetFileIO
from .pickle import PickleFileIO
from .text import TextFileIO
from .sql import SQLFileIO
from .yaml import YamlFileIO

fileio_mapping = {
    "csv": CSVFileIO,
    "txt": TextFileIO,
    "text": TextFileIO,
    "log": TextFileIO,
    "sql": SQLFileIO,
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
        # Validate and cache the file extension during initialization
        self.file_extension = self._validate_file_extension()

    def _fexists(self, *args, **kwargs) -> bool:
        """
        Check if the file exists.
        """
        return self.upath.exists()

    def _finfo(self, *args, **kwargs) -> dict:
        """
        Get file information.
        """
        try:
            return self.upath.fs.info(self.upath.path, *args, **kwargs)
        except OSError as e:
            warnings.warn(f"{self.upath.path} does not exist or is not accessible.")
            raise e
        
    def _validate_file_extension(self) -> str:
        """
        Validate and return the file extension.
        
        Returns:
            str: The validated file extension (lowercase, without dot).
            
        Raises:
            ValueError: If file has no extension or unsupported format.
        """
        file_extension = self.upath.suffix[1:].lower()
        if not file_extension:
            raise ValueError(f"File {self.upath.path} has no extension")
        if file_extension not in fileio_mapping:
            raise ValueError(f"Unsupported file format: .{file_extension}.\n"
                             f"Supported formats: {list(fileio_mapping.keys())}")
        return file_extension

    def _fread(self, offset: int = 0, size: Optional[int] = None, raw_bytes: bool = False, *args, **kwargs) -> object:
        """
        Read the file content.
        
        Args:
            offset (int): Offset in bytes from the beginning of the file. Defaults to 0.
            size (Optional[int]): Number of bytes to read. If None, reads entire file.
            raw_bytes (bool): If True, returns raw bytes instead of parsed content.
        
        Returns:
            object: Parsed file content or raw bytes if raw_bytes=True.
        """
        # Check if file exists
        if not self.upath.exists():
            raise FileNotFoundError(f"File not found: {self.upath.path}")
        
        with self.upath.fs.open(self.upath.path, 'rb') as f:
            f.seek(offset)
            if size is not None:
                raw_data = f.read(size)
            else:
                raw_data = f.read(*args, **kwargs)
        
        # Return raw bytes if requested
        if raw_bytes:
            return raw_data
        
        # Otherwise, parse according to file format
        data: BytesIO = BytesIO(raw_data)
        file_io_cls = fileio_mapping[self.file_extension]
        return file_io_cls._read(data)
    
    def _fopen(self, mode: str = 'r', *args, **kwargs):
        """
        Open the file for streaming operations (reading/writing in chunks).
        
        Args:
            mode (str): File mode ('r', 'w', 'a', 'rb', 'wb', 'ab', etc.). Defaults to 'r'.
            
        Returns:
            File-like object: An opened file handle for streaming operations.
            
        Note:
            This method bypasses the format-specific FileIO classes and provides
            direct access to the underlying filesystem for streaming operations.
            Use this for cases where you need to read/write files incrementally
            rather than loading entire content into memory.
        """
        return self.upath.fs.open(self.upath.path, mode, *args, **kwargs)
    
    def _fcopy(self, dest_path: str, *args, **kwargs) -> None:
        """
        Copy the file to a new location.
        
        Args:
            dest_path (str): Destination path for the copied file.
        """
        # Check if source file exists
        if not self.upath.exists():
            raise FileNotFoundError(f"Source file not found: {self.upath.path}")
            
        # Validate destination path
        if not dest_path or not dest_path.strip():
            raise ValueError("Destination path cannot be empty")
            
        try:
            dest_upath: UPath = UPath(dest_path)
            with self.upath.fs.open(self.upath.path, 'rb') as src_file, dest_upath.fs.open(dest_upath.path, 'wb') as dest_file:
                dest_file.write(src_file.read(*args, **kwargs))
            # return self.upath.fs.copy(self.upath.path, dest_upath.path, *args, **kwargs
        except OSError as e:
            warnings.warn(f"Failed to copy {self.upath.path} to {dest_path}: {e}")
            raise e

    def _fwrite(self, data: object, mode: str = None, raw_bytes: bool = False, *args, **kwargs) -> None:
        """
        Write data to the file.
        
        Args:
            data (object): Data to write to the file.
            mode (str): Mode to open the file. If not provided, auto-detected based on file type.
            raw_bytes (bool): If True, writes raw bytes directly without format-specific processing.
        """
        # Handle raw bytes writing (for file copying)
        if raw_bytes:
            if not isinstance(data, bytes):
                raise TypeError("Data must be bytes when raw_bytes=True")
            
            if mode is None:
                mode = 'wb'  # Default to write binary for raw bytes
            
            with self.upath.fs.open(self.upath.path, mode) as f:
                f.write(data, *args, **kwargs)
            return
        
        self._validate_data_type(data, self.file_extension)

        # Set default mode based on file extension if not provided
        if mode is None:
            # Binary formats (actual binary file formats)
            binary_exts = {'feather', 'parquet', 'arrow', 'pickle', 'pkl'}
            # Text formats (including CSV which is text-based)
            text_exts = {'csv', 'txt', 'text', 'sql', 'log', 'json', 'yaml', 'yml'}
            if self.file_extension in binary_exts:
                mode = 'wb'
            elif self.file_extension in text_exts:
                mode = 'w'
            else:
                # Fallback to text mode
                mode = 'w'

        file_io_cls = fileio_mapping[self.file_extension]
        return file_io_cls._write(self.upath, data, mode=mode, *args, **kwargs)
    
    def _validate_data_type(self, data: object, file_extension: str) -> None:
        """
        Validate that the data type is appropriate for the file format.
        
        Args:
            data: The data to validate
            file_extension: The file extension to check against
        """
        from pandas import DataFrame
        
        # Define required types for each format
        dataframe_formats = {'csv', 'feather', 'parquet', 'arrow'}
        str_formats = {'txt', 'text', 'sql', 'log'}
        serializable_formats = {'json', 'yaml', 'yml', 'pickle', 'pkl'}

        # JSON and YAML can accept various serializable types, so we don't restrict them
        # Pickle can accept any object, so no validation needed
        if file_extension in dataframe_formats:
            if not isinstance(data, DataFrame):
                raise TypeError(f"Writing {file_extension.upper()} files requires a pandas DataFrame, got {type(data).__name__}")
        
        elif file_extension in str_formats:
            if not isinstance(data, str):
                raise TypeError(f"Writing {file_extension.upper()} files requires a string, got {type(data).__name__}")
        
        elif file_extension in serializable_formats:
            pass  # JSON, YAML, and Pickle can handle various serializable types
        
    # def _fmakedirs(self, dirpath: str, exist_ok: bool = True, *args, **kwargs) -> None:
    #     """
    #     Create directories for the file path recursively.
        
    #     Args:
    #         dirpath (str): Directory path to create.
    #         exist_ok (bool): If True, do not raise an error if the directory already exists.

    #     Raises:
    #         OSError: If the directory cannot be created.
    #     """
    #     try:
    #         self.upath.fs.makedirs(dirpath, exist_ok=exist_ok, *args, **kwargs)
    #     except OSError as e:
    #         warnings.warn(f"Failed to create directories for {dirpath}: {e}")
    #         raise e

    def _fdelete(self, filepath: str, *args, **kwargs) -> None:
        """
        Delete the specified file or directory.

        Args:
            filepath (str): Path to the file or directory to delete.

        Raises:
            OSError: If the file or directory cannot be deleted.
        """
        # Validate filepath
        if not filepath or not filepath.strip():
            raise ValueError("File path cannot be empty")
            
        # Check if file/directory exists
        delete_upath = UPath(filepath)
        if not delete_upath.exists():
            warnings.warn(f"Path does not exist: {filepath}")
            return  # Don't raise error for non-existent files
            
        try:
            # Use the same filesystem instance for consistency
            delete_upath.fs.rm(delete_upath.path, *args, **kwargs)
        except OSError as e:
            warnings.warn(f"Failed to delete {filepath}: {e}")
            raise e