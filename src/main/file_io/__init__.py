import fsspec

from ._base import BaseFileIO
from .._aux import retry_args

from upath import UPath
from typing import Union, Optional, Any, Union, overload
class FileIOInterface:
    @staticmethod
    def _instantiate(fpath: str, filesystem: Optional[str] = None, *args, **kwargs) -> BaseFileIO:
        """
        Instantiate a BaseFileIO object for the given file path and filesystem.
        
        Args:
            fpath (str): File path to instantiate.
            filesystem (Optional[str]): Filesystem type, if any.

        Returns:
            BaseFileIO: An instance of BaseFileIO or its subclass.
        
        Raises:
            ValueError: If the filesystem is unsupported.
        """
        if filesystem is not None and filesystem not in fsspec.available_protocols():
            raise ValueError(f"Unsupported filesystem: {filesystem}")
        upath_obj: UPath = UPath(fpath, protocol=filesystem)
        fileio: BaseFileIO = BaseFileIO(upath_obj=upath_obj, *args, **kwargs)
        return fileio

    @staticmethod
    @retry_args
    def fexists(fpath: str, filesystem: Optional[str] = None, *args, **kwargs) -> bool:
        """
        Check if a file exists at the specified path.

        Args:
            fpath (str): File path to check.
            filesystem (Optional[str]): Filesystem type, if any. Defaults to None.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        fileio: BaseFileIO = __class__._instantiate(fpath, filesystem, *args, **kwargs)
        return fileio._fexists(*args, **kwargs)

    @staticmethod
    @retry_args
    def finfo(fpath: str, filesystem: Optional[str] = None, *args, **kwargs) -> Union[dict, None]:
        """
        Get file information for the specified file path.
        
        Args:
            fpath (str): File path to get information for.
            filesystem (Optional[str]): Filesystem type, if any.

        Returns:
            Union[dict, None]: File information or None if not accessible.
        """
        fileio: BaseFileIO = __class__._instantiate(fpath=fpath, filesystem=filesystem, *args, **kwargs)
        return fileio._finfo(*args, **kwargs)
    
    @staticmethod
    @retry_args
    def fread(read_path: str, filesystem: Optional[str] = None, *args, **kwargs) -> Any:
        """
        Read the content of a file at the specified path.
        
        Args:
            read_path (str): Path to the file to read.
            filesystem (Optional[str]): Filesystem type, if any.

        Returns:
            Any: Parsed file content.
        """
        fileio: BaseFileIO = __class__._instantiate(fpath=read_path, filesystem=filesystem, *args, **kwargs)
        return fileio._fread(*args, **kwargs)
    
    @staticmethod
    @retry_args
    def fcopy(read_path: str, dest_path: str, filesystem: Optional[str] = None, *args, **kwargs) -> None:
        """
        Copy a file from one path to another.
        
        Args:
            read_path (str): Path to the source file.
            dest_path (str): Path to the destination file.
            filesystem (Optional[str]): Filesystem type, if any.
        """
        fileio: BaseFileIO = __class__._instantiate(fpath=read_path, filesystem=filesystem, *args, **kwargs)
        return fileio._fcopy(dest_path=dest_path, *args, **kwargs)
    
    @staticmethod
    @overload
    def fwrite(write_path: str, data, filesystem: Optional[str] = None, *args, **kwargs) -> None:
        """Write DataFrame to CSV, Feather, Parquet, or Arrow file."""
        ...
    
    @staticmethod
    @overload
    def fwrite(write_path: str, data: str, filesystem: Optional[str] = None, *args, **kwargs) -> None:
        """Write string to text file."""
        ...
    
    @staticmethod
    @overload
    def fwrite(write_path: str, data: Any, filesystem: Optional[str] = None, *args, **kwargs) -> None:
        """Write any serializable data to JSON, YAML, or Pickle file."""
        ...
    
    @staticmethod
    @retry_args
    def fwrite(write_path: str, data: Any, filesystem: Optional[str] = None, *args, **kwargs) -> None:
        """
        Write data to a file at the specified path.
        
        Args:
            write_path (str): Path to the file to write.
            data (Any): Data to write to the file. Type must match file format:
                - DataFrame: for .csv, .feather, .parquet, .arrow files
                - str: for .txt/.text files
                - Any serializable: for .json, .yaml/.yml, .pickle/.pkl files
            filesystem (Optional[str]): Filesystem type, if any.
        
        Raises:
            TypeError: If data type doesn't match the expected type for the file format.
        """
        fileio: BaseFileIO = __class__._instantiate(fpath=write_path, filesystem=filesystem, *args, **kwargs)
        return fileio._fwrite(data=data, *args, **kwargs)
    
    @staticmethod
    @retry_args
    def fmakedirs(path: str, filesystem: Optional[str] = None, exist_ok: bool = True, *args, **kwargs) -> None:
        """
        Create directories at the specified path.
        
        Args:
            path (str): Path to create directories at.
            filesystem (Optional[str]): Filesystem type, if any.
            exist_ok (bool): If True, do not raise an error if the directory already exists.
        """
        fileio: BaseFileIO = __class__._instantiate(fpath=path, filesystem=filesystem, *args, **kwargs)
        return fileio._fmakedirs(dirpath=path, exist_ok=exist_ok, *args, **kwargs)

    @staticmethod
    @retry_args
    def fdelete(path: str, filesystem: Optional[str] = None, *args, **kwargs) -> None:
        """
        Delete the specified file or directory.
        
        Args:
            path (str): Path to the file or directory to delete.
            filesystem (Optional[str]): Filesystem type, if any.
        """
        # Use UPath to check if it's a directory
        upath_obj = UPath(path, protocol=filesystem)
        if upath_obj.is_dir():
            # Remove directory and all contents
            upath_obj.fs.rm(upath_obj.path, recursive=True)
        else:
            fileio: BaseFileIO = __class__._instantiate(fpath=path, filesystem=filesystem, *args, **kwargs)
            return fileio._fdelete(filepath=path, *args, **kwargs)