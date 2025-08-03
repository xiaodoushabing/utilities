"""
Configuration and fixtures for FileIO tests.

This file contains shared fixtures and configuration used across FileIO test modules.
It demonstrates pytest best practices for test organization and reusable components.
"""

import pytest
import os
import tempfile
import json
import pandas as pd
from pathlib import Path


@pytest.fixture(scope="function")
def temp_directory(tmpdir):
    """
    Create a temporary directory for file operations.
    
    PYTEST: Function-scoped fixture that creates a fresh temporary directory
    for each test function. Automatically cleaned up after each test.
    """
    return str(tmpdir)


@pytest.fixture(scope="session")
def sample_datasets():
    """
    Sample datasets for testing various file formats.
    
    PYTEST: Session-scoped fixture that creates data once per test session.
    Use this for expensive-to-create data that doesn't change between tests.
    """
    return {
        "simple_json": {
            "name": "test_app",
            "version": "1.0.0",
            "active": True
        },
        "complex_json": {
            "metadata": {
                "created": "2024-01-01",
                "author": "test_user"
            },
            "data": [
                {"id": 1, "value": "first"},
                {"id": 2, "value": "second"}
            ],
            "settings": {
                "debug": False,
                "timeout": 30
            }
        },
        "simple_text": "Hello, World!\nThis is a test file.",
        "multiline_text": """Line 1
Line 2 with special chars: @#$%^&*()
Line 3 with unicode: 🚀 αβγ 中文
Line 4 with tabs\tand\tspaces
Final line""",
        "simple_dataframe": pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "city": ["NYC", "SF", "CHI"]
        }),
        "numeric_dataframe": pd.DataFrame({
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "bool_col": [True, False, True, False, True],
            "str_col": ["a", "b", "c", "d", "e"]
        }),
        "yaml_config": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "credentials": {
                    "username": "admin",
                    "password": "secret"
                }
            },
            "logging": {
                "level": "INFO",
                "handlers": ["console", "file"]
            },
            "features": {
                "auth": True,
                "caching": False,
                "monitoring": True
            }
        }
    }


@pytest.fixture(scope="function")
def file_samples(temp_directory, sample_datasets):
    """
    Create sample files in temporary directory.
    
    PYTEST: Function-scoped fixture that creates actual files for integration testing.
    Returns paths to the created files for easy access in tests.
    """
    import yaml
    
    file_paths = {}
    
    # Create JSON files
    json_simple_path = os.path.join(temp_directory, "simple.json")
    with open(json_simple_path, 'w') as f:
        json.dump(sample_datasets["simple_json"], f)
    file_paths["json_simple"] = json_simple_path
    
    json_complex_path = os.path.join(temp_directory, "complex.json")
    with open(json_complex_path, 'w') as f:
        json.dump(sample_datasets["complex_json"], f, indent=2)
    file_paths["json_complex"] = json_complex_path
    
    # Create text files
    text_simple_path = os.path.join(temp_directory, "simple.txt")
    with open(text_simple_path, 'w', encoding='utf-8') as f:
        f.write(sample_datasets["simple_text"])
    file_paths["text_simple"] = text_simple_path
    
    text_multiline_path = os.path.join(temp_directory, "multiline.txt")
    with open(text_multiline_path, 'w', encoding='utf-8') as f:
        f.write(sample_datasets["multiline_text"])
    file_paths["text_multiline"] = text_multiline_path
    
    # Create CSV files
    csv_simple_path = os.path.join(temp_directory, "simple.csv")
    sample_datasets["simple_dataframe"].to_csv(csv_simple_path, index=False)
    file_paths["csv_simple"] = csv_simple_path
    
    csv_numeric_path = os.path.join(temp_directory, "numeric.csv")
    sample_datasets["numeric_dataframe"].to_csv(csv_numeric_path, index=False)
    file_paths["csv_numeric"] = csv_numeric_path
    
    # Create YAML files
    yaml_config_path = os.path.join(temp_directory, "config.yaml")
    with open(yaml_config_path, 'w') as f:
        yaml.dump(sample_datasets["yaml_config"], f, default_flow_style=False)
    file_paths["yaml_config"] = yaml_config_path
    
    return file_paths


@pytest.fixture(scope="function")
def corrupted_files(temp_directory):
    """
    Create corrupted/malformed files for error testing.
    
    PYTEST: Function-scoped fixture for testing error handling.
    These files contain intentionally malformed data to test error conditions.
    """
    corrupted_paths = {}
    
    # Corrupted JSON
    json_corrupted_path = os.path.join(temp_directory, "corrupted.json")
    with open(json_corrupted_path, 'w') as f:
        f.write('{"invalid": json, "missing": "quotes"}')
    corrupted_paths["json_corrupted"] = json_corrupted_path
    
    # Corrupted YAML
    yaml_corrupted_path = os.path.join(temp_directory, "corrupted.yaml")
    with open(yaml_corrupted_path, 'w') as f:
        f.write('invalid: yaml:\n  bad_indentation: error\n wrong_level: value')
    corrupted_paths["yaml_corrupted"] = yaml_corrupted_path
    
    # Binary file with .txt extension (will cause decode errors)
    binary_text_path = os.path.join(temp_directory, "binary.txt")
    with open(binary_text_path, 'wb') as f:
        f.write(b'\xff\xfe\x00\x00\x01\x02\x03\x04')
    corrupted_paths["binary_text"] = binary_text_path
    
    # Empty files
    empty_json_path = os.path.join(temp_directory, "empty.json")
    with open(empty_json_path, 'w') as f:
        pass  # Create empty file
    corrupted_paths["empty_json"] = empty_json_path
    
    return corrupted_paths


@pytest.fixture(scope="function") 
def readonly_directory(temp_directory):
    """
    Create a read-only directory for permission testing.
    
    PYTEST: Platform-aware fixture for testing permission scenarios.
    Note: Permission testing can be platform-specific.
    """
    readonly_path = os.path.join(temp_directory, "readonly")
    os.makedirs(readonly_path, exist_ok=True)
    
    # Make directory read-only (platform dependent)
    if os.name != 'nt':  # Unix-like systems
        os.chmod(readonly_path, 0o444)
    
    yield readonly_path
    
    # Cleanup: restore write permissions for deletion
    if os.name != 'nt':
        os.chmod(readonly_path, 0o755)


@pytest.fixture(autouse=True)
def cleanup_test_files():
    """
    Auto-cleanup fixture that runs after each test.
    
    PYTEST: autouse=True means this fixture runs automatically for every test.
    Use this for global cleanup that should happen regardless of test outcome.
    """
    yield  # Test execution happens here
    
    # Cleanup any leftover test files (in case temp directories weren't cleaned)
    # This is a safety net - normally tmpdir handles cleanup
    pass


# Pytest configuration
def pytest_configure(config):
    """
    Pytest configuration hook.
    
    PYTEST: This function is called during pytest startup.
    Use it to configure markers, plugins, or other pytest behavior.
    """
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (slower, use real files)"
    )
    config.addinivalue_line(
        "markers", 
        "unit: marks tests as unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers",
        "error_handling: marks tests that focus on error conditions"
    )


# Custom pytest markers for test organization
pytestmark = [
    pytest.mark.fileio,  # All tests in this package are fileio tests
]
