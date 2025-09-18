"""
Shared pytest fixtures for FileIO test suite.

This file centralizes common test setup and teardown logic to eliminate
duplication across test files. All fixtures defined here are automatically
available to all test files in this directory.
"""

from unittest.mock import MagicMock
import sys

# Patch hydra.logging.promtail.PromtailAgent before any imports
sys.modules['hydra'] = MagicMock()
sys.modules['hydra.logging'] = MagicMock()
sys.modules['hydra.logging.promtail'] = MagicMock()
sys.modules['hydra.logging.promtail'].PromtailAgent = MagicMock()

import pytest
import tempfile
import shutil
import json
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.main.file_io import FileIOInterface

# ========================================================================================
# TEMPORARY DIRECTORY FIXTURES
# ========================================================================================

@pytest.fixture
def temp_dir():
    """
    Creates a temporary folder for testing.
    
    - Auto-cleanup after test completes
    - Safe place for test files
    
    Usage: def test_something(temp_dir):
           file_path = os.path.join(temp_dir, 'test.txt')
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def temp_file_path(temp_dir):
    """
    Provides a temporary file path (file doesn't exist yet).
    
    Usage: def test_something(temp_file_path):
           # temp_file_path is a string path in temp directory
    """
    return str(Path(temp_dir) / "test_file.txt")


# ========================================================================================
# SAMPLE DATA FIXTURES
# ========================================================================================

@pytest.fixture
def sample_dataframe():
    """Creates a sample pandas DataFrame for testing."""
    return pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['New York', 'London', 'Tokyo']
    })


@pytest.fixture
def sample_json_data():
    """Creates sample JSON-serializable data for testing."""
    return {
        'users': [
            {'id': 1, 'name': 'Alice', 'active': True},
            {'id': 2, 'name': 'Bob', 'active': False}
        ],
        'metadata': {
            'version': '1.0',
            'created': '2025-01-01'
        }
    }


@pytest.fixture
def sample_yaml_data():
    """Creates sample YAML-serializable data for testing."""
    return {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'name': 'testdb'
        },
        'features': ['logging', 'caching', 'monitoring']
    }


@pytest.fixture
def sample_text_data():
    """Creates sample text data for testing."""
    return "This is a test file.\nIt contains multiple lines.\nUsed for testing text I/O operations."

# ========================================================================================
# MOCK FIXTURES
# ========================================================================================

@pytest.fixture
def mock_upath(mock_file_context):
    """Creates a mock UPath object for testing."""
    mock_upath = MagicMock()
    mock_upath.path = "/test/path/file.txt"
    mock_upath.suffix = ".txt"
    mock_upath.exists.return_value = True
    
    # Set up the file system mock to return our mock file context
    mock_fs = MagicMock()
    mock_fs.open.return_value.__enter__.return_value = mock_file_context
    mock_upath.fs = mock_fs
    
    return mock_upath


@pytest.fixture
def mock_fsspec():
    """Mocks fsspec.available_protocols() for testing filesystem validation."""
    with patch('fsspec.available_protocols') as mock_protocols:
        mock_protocols.return_value = ['file', 's3', 'gcs', 'hdfs', 'http', 'https']
        yield mock_protocols


@pytest.fixture
def mock_fileio_mapping():
    """
    Mocks the fileio_mapping dictionary used in BaseFileIO._fread and _fwrite methods.
    
    Returns a context manager that provides a mock file IO class with configurable behavior.
    """
    with patch('src.main.file_io._base.fileio_mapping') as mock_mapping:
        # Create a mock file IO class that can be configured per test
        mock_io_class = MagicMock()
        mock_mapping.__contains__.return_value = True
        mock_mapping.__getitem__.return_value = mock_io_class
        yield mock_io_class

@pytest.fixture
def mock_file_context():
    """
    Provides a reusable mock file object with context manager support.
    """
    mock_file = MagicMock()
    mock_file.read.return_value = b"default test content"
    mock_file.__enter__ = MagicMock(return_value=mock_file)
    mock_file.__exit__ = MagicMock(return_value=None)
    
    # Reset the mock between tests to prevent state leakage
    yield mock_file
    mock_file.reset_mock()

@pytest.fixture
def mock_base_fileio(mock_upath):
    """Creates a mock BaseFileIO instance for testing."""
    with patch('src.main.file_io._base.BaseFileIO') as mock_class:
        mock_instance = MagicMock()
        mock_instance.upath = mock_upath
        mock_instance.file_extension = mock_upath.suffix.lstrip('.')
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_instantiate():
    """
    Provides a mock for FileIOInterface._instantiate method.
    
    This fixture automatically mocks the _instantiate method and returns a configured
    mock FileIO object that can be used across tests. This eliminates the need to 
    manually patch _instantiate in every test method.
    
    Returns:
        dict: Contains 'mock' (the patch object) and 'fileio' (mock FileIO instance)
        
    Usage:
        def test_something(mock_instantiate):
            mock_instantiate['fileio']._fread.return_value = "test data"
            result = FileIOInterface.fread("/test/file.txt")
            mock_instantiate['mock'].assert_called_once_with("/test/file.txt", None)
    """
    with patch.object(FileIOInterface, '_instantiate') as mock_instantiate_patch:
        # Create a mock FileIO object with common methods
        mock_fileio = MagicMock()
        
        # Set up default return values for common methods
        mock_fileio._fexists.return_value = True
        mock_fileio._finfo.return_value = {"size": 1024, "type": "file"}
        mock_fileio._fread.return_value = "default test data"
        mock_fileio._fwrite.return_value = None
        mock_fileio._fcopy.return_value = None
        mock_fileio._fdelete.return_value = None
        
        # Configure the instantiate mock to return our FileIO mock
        mock_instantiate_patch.return_value = mock_fileio
        
        yield {
            'mock': mock_instantiate_patch,
            'fileio': mock_fileio
        }


@pytest.fixture
def mock_file_operations(mock_upath, mock_base_fileio):
    """Mocks common file operations to avoid real file system access."""
    with patch('src.main.file_io.UPath') as mock_upath_class, \
         patch('src.main.file_io.BaseFileIO') as mock_baseio_class:
        
        mock_upath_class.return_value = mock_upath
        mock_baseio_class.return_value = mock_base_fileio
        
        yield {
            'upath_class': mock_upath_class,
            'upath_instance': mock_upath,
            'baseio_class': mock_baseio_class,
            'baseio_instance': mock_base_fileio
        }


# ========================================================================================
# PARAMETRIZED FIXTURES FOR TESTING MULTIPLE FILE FORMATS
# ========================================================================================

@pytest.fixture(params=[
    # Text-based formats (require string data)
    'txt', 'text', 'log', 'logs', 'sql',
    # JSON/YAML formats (require serializable data)
    'json', 'yaml', 'yml',
    # DataFrame formats (require pandas DataFrame)
    'csv', 'parquet', 'arrow', 'feather',
    # Pickle formats (can handle any serializable data)
    'pickle', 'pkl'
])
def file_extension(request):
    """Parametrized fixture for testing different file extensions.
    
    Covers all supported file extensions from the fileio_mapping:
    - Text formats: txt, text, log, logs, sql
    - Serializable formats: json, yaml, yml
    - DataFrame formats: csv, parquet, arrow, feather  
    - Pickle formats: pickle, pkl
    """
    return request.param


@pytest.fixture
def file_path_by_extension(temp_dir, file_extension):
    """Provides file paths for different extensions."""
    return str(Path(temp_dir) / f"test.{file_extension}")


@pytest.fixture
def sample_data_by_extension(file_extension, sample_json_data, sample_yaml_data, 
                           sample_text_data, sample_dataframe):
    """Provides appropriate sample data based on file extension.
    
    Maps each file extension to the correct data type:
    - Text formats (txt, text, log, logs, sql): string data
    - Serializable formats (json, yaml, yml): dict/list data
    - DataFrame formats (csv, parquet, arrow, feather): pandas DataFrame
    - Pickle formats (pickle, pkl): any serializable data (using dict here)
    """
    # Text-based formats require string data
    text_formats = {'txt', 'text', 'log', 'logs','sql'}
    # DataFrame formats require pandas DataFrame
    dataframe_formats = {'csv', 'parquet', 'arrow', 'feather'}
    # JSON/YAML formats work with serializable data (dict/list)
    serializable_formats = {'json', 'yaml', 'yml'}
    # Pickle formats can handle any serializable data
    pickle_formats = {'pickle', 'pkl'}
    
    if file_extension in text_formats:
        return sample_text_data
    elif file_extension in dataframe_formats:
        return sample_dataframe
    elif file_extension in serializable_formats:
        # Use YAML data for JSON (both support dicts/lists)
        return sample_yaml_data if file_extension in {'yaml', 'yml'} else sample_json_data
    elif file_extension in pickle_formats:
        # Pickle can handle any data type - use complex nested data
        return sample_json_data
    else:
        raise ValueError(f"Unsupported file extension for test data: {file_extension}")


# ========================================================================================
# CREATED FILE FIXTURES
# ========================================================================================

@pytest.fixture
def json_file_path(temp_dir):
    """Provides a JSON file path."""
    return str(Path(temp_dir) / "test.json")

@pytest.fixture
def existing_json_file(json_file_path, sample_json_data):
    """Creates an actual JSON file for testing read operations."""
    with open(json_file_path, 'w') as f:
        json.dump(sample_json_data, f)
    return json_file_path


# ========================================================================================
# RETRY TESTING FIXTURES
# ========================================================================================

@pytest.fixture
def mock_retry_decorator():
    """Mocks the retry_args decorator for testing retry functionality."""
    with patch('src.main._aux._aux.Retrying') as mock_retrying:
        mock_instance = MagicMock()
        mock_retrying.return_value = mock_instance
        
        def mock_retry_call(func, *args, **kwargs):
            return func(*args, **kwargs)
        mock_instance.side_effect = mock_retry_call
        
        yield mock_retrying


@pytest.fixture
def failing_then_succeeding_operation():
    """Creates a mock operation that fails on first attempt then succeeds."""
    call_count = 0
    
    def operation(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("Temporary failure")
        return "Success"
    
    return operation
