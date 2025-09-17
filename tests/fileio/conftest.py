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
    - Text formats: txt, text, log, sql
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


@pytest.fixture
def arrow_file_path(temp_dir):
    """Provides an Arrow file path."""
    return str(Path(temp_dir) / "test.arrow")


@pytest.fixture
def feather_file_path(temp_dir):
    """Provides a Feather file path."""
    return str(Path(temp_dir) / "test.feather")


@pytest.fixture
def sql_file_path(temp_dir):
    """Provides a SQL file path."""
    return str(Path(temp_dir) / "test.sql")


@pytest.fixture
def log_file_path(temp_dir):
    """Provides a log file path."""
    return str(Path(temp_dir) / "test.log")

@pytest.fixture
def logs_file_path(temp_dir):
    """Provides a log file path."""
    return str(Path(temp_dir) / "test.logs")

# ========================================================================================
# CREATED FILE FIXTURES
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


@pytest.fixture
def existing_parquet_file(parquet_file_path, sample_dataframe):
    """Creates an actual Parquet file for testing read operations."""
    sample_dataframe.to_parquet(parquet_file_path, index=False)
    return parquet_file_path


@pytest.fixture
def existing_feather_file(feather_file_path, sample_dataframe):
    """Creates an actual Feather file for testing read operations."""
    sample_dataframe.to_feather(feather_file_path)
    return feather_file_path


@pytest.fixture
def existing_arrow_file(arrow_file_path, sample_dataframe):
    """Creates an actual Arrow file for testing read operations."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    table = pa.Table.from_pandas(sample_dataframe)
    pq.write_table(table, arrow_file_path)
    return arrow_file_path


@pytest.fixture
def existing_pickle_file(pickle_file_path, sample_json_data):
    """Creates an actual Pickle file for testing read operations."""
    import pickle
    with open(pickle_file_path, 'wb') as f:
        pickle.dump(sample_json_data, f)
    return pickle_file_path


@pytest.fixture
def existing_sql_file(sql_file_path):
    """Creates an actual SQL file for testing read operations."""
    sql_content = """-- Sample SQL script
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);

INSERT INTO users (name, email) VALUES 
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');
"""
    with open(sql_file_path, 'w') as f:
        f.write(sql_content)
    return sql_file_path


@pytest.fixture
def existing_log_file(log_file_path):
    """Creates an actual log file for testing read operations."""
    log_content = """2025-01-01 10:00:00 INFO Starting application
2025-01-01 10:00:01 DEBUG Database connection established
2025-01-01 10:00:02 WARNING Retrying failed operation
2025-01-01 10:00:03 ERROR Operation failed after 3 retries
"""
    with open(log_file_path, 'w') as f:
        f.write(log_content)
    return log_file_path

@pytest.fixture
def existing_logs_file(logs_file_path):
    """Creates an actual logs file for testing read operations."""
    logs_content = """2025-01-01 10:00:00 INFO Starting application
2025-01-01 10:00:01 DEBUG Database connection established
2025-01-01 10:00:02 WARNING Retrying failed operation
2025-01-01 10:00:03 ERROR Operation failed after 3 retries
"""
    with open(logs_file_path, 'w') as f:
        f.write(logs_content)
    return logs_file_path

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
