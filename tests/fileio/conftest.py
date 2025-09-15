"""
Shared pytest fixtures for FileIO test suite.

This file centralizes common test setup and teardown logic to eliminate
duplication across test files. All fixtures defined here are automatically
available to all test files in this directory.
"""

import pytest
import tempfile
import shutil
import json
import yaml
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from io import BytesIO

from src.main.file_io import FileIOInterface
from src.main.file_io._base import BaseFileIO


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
# MOCK FIXTURES - CENTRALIZED TO ELIMINATE DUPLICATION
# ========================================================================================

@pytest.fixture
def mock_upath():
    """Creates a mock UPath object for testing."""
    mock_upath = MagicMock()
    mock_upath.path = "/test/path/file.txt"
    mock_upath.suffix = ".txt"
    mock_upath.exists.return_value = True
    
    # Mock filesystem
    mock_fs = MagicMock()
    mock_upath.fs = mock_fs
    
    return mock_upath


@pytest.fixture
def mock_fsspec():
    """Mocks fsspec.available_protocols() for testing filesystem validation."""
    with patch('fsspec.available_protocols') as mock_protocols:
        mock_protocols.return_value = ['file', 's3', 'gcs', 'hdfs', 'http', 'https']
        yield mock_protocols


@pytest.fixture
def mock_base_fileio(mock_upath):
    """Creates a mock BaseFileIO instance for testing."""
    with patch('src.main.file_io._base.BaseFileIO') as mock_class:
        mock_instance = MagicMock()
        mock_instance.upath = mock_upath
        mock_instance.file_extension = "txt"
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture  
def mock_fileio_interface():
    """Creates a mock for FileIOInterface._instantiate method."""
    with patch.object(FileIOInterface, '_instantiate') as mock_instantiate:
        mock_fileio = MagicMock()
        mock_instantiate.return_value = mock_fileio
        yield mock_fileio


@pytest.fixture
def mock_file_operations():
    """Mocks common file operations to avoid real file system access."""
    with patch('src.main.file_io.UPath') as mock_upath_class, \
         patch('src.main.file_io.BaseFileIO') as mock_baseio_class:
        
        mock_upath_instance = MagicMock()
        mock_upath_instance.exists.return_value = True
        mock_upath_instance.suffix = ".txt"
        mock_upath_class.return_value = mock_upath_instance
        
        mock_baseio_instance = MagicMock()
        mock_baseio_class.return_value = mock_baseio_instance
        
        yield {
            'upath_class': mock_upath_class,
            'upath_instance': mock_upath_instance,
            'baseio_class': mock_baseio_class,
            'baseio_instance': mock_baseio_instance
        }


# ========================================================================================
# PARAMETRIZED FIXTURES FOR TESTING MULTIPLE FILE FORMATS
# ========================================================================================

@pytest.fixture(params=['json', 'yaml', 'txt', 'csv', 'parquet', 'pkl'])
def file_extension(request):
    """Parametrized fixture for testing different file extensions."""
    return request.param


@pytest.fixture
def file_path_by_extension(temp_dir, file_extension):
    """Provides file paths for different extensions."""
    return str(Path(temp_dir) / f"test.{file_extension}")


@pytest.fixture
def sample_data_by_extension(file_extension, sample_json_data, sample_yaml_data, 
                           sample_text_data, sample_dataframe):
    """Provides appropriate sample data based on file extension."""
    data_mapping = {
        'json': sample_json_data,
        'yaml': sample_yaml_data,
        'txt': sample_text_data,
        'csv': sample_dataframe,
        'parquet': sample_dataframe,
        'pkl': sample_json_data  # pickle can handle any serializable data
    }
    return data_mapping[file_extension]


# ========================================================================================
# FILE FIXTURES WITH DIFFERENT EXTENSIONS
# ========================================================================================

@pytest.fixture
def csv_file_path(temp_dir):
    """Provides a CSV file path."""
    return str(Path(temp_dir) / "test.csv")


@pytest.fixture
def json_file_path(temp_dir):
    """Provides a JSON file path."""
    return str(Path(temp_dir) / "test.json")


@pytest.fixture
def yaml_file_path(temp_dir):
    """Provides a YAML file path."""
    return str(Path(temp_dir) / "test.yaml")


@pytest.fixture
def txt_file_path(temp_dir):
    """Provides a text file path."""
    return str(Path(temp_dir) / "test.txt")


@pytest.fixture
def parquet_file_path(temp_dir):
    """Provides a Parquet file path."""
    return str(Path(temp_dir) / "test.parquet")


@pytest.fixture
def pickle_file_path(temp_dir):
    """Provides a Pickle file path."""
    return str(Path(temp_dir) / "test.pkl")


# ========================================================================================
# CREATED FILE FIXTURES (Files that actually exist) - FOR INTEGRATION TESTS
# ========================================================================================

@pytest.fixture
def existing_json_file(json_file_path, sample_json_data):
    """Creates an actual JSON file for testing read operations."""
    with open(json_file_path, 'w') as f:
        json.dump(sample_json_data, f)
    return json_file_path


@pytest.fixture
def existing_yaml_file(yaml_file_path, sample_yaml_data):
    """Creates an actual YAML file for testing read operations."""
    with open(yaml_file_path, 'w') as f:
        yaml.dump(sample_yaml_data, f)
    return yaml_file_path


@pytest.fixture
def existing_text_file(txt_file_path, sample_text_data):
    """Creates an actual text file for testing read operations."""
    with open(txt_file_path, 'w') as f:
        f.write(sample_text_data)
    return txt_file_path


@pytest.fixture
def existing_csv_file(csv_file_path, sample_dataframe):
    """Creates an actual CSV file for testing read operations."""
    sample_dataframe.to_csv(csv_file_path, index=False)
    return csv_file_path


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
