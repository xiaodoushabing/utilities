"""
Unit tests for specific FileIO format implementations.

This test suite focuses on testing individual FileIO format classes:
- JsonFileIO for JSON file operations
- TextFileIO for text file operations  
- CSVFileIO for CSV file operations
- YamlFileIO for YAML file operations

Key testing patterns:
📚 Direct class testing: Testing each format class independently
🔄 BytesIO simulation: Testing read operations with in-memory data
🎭 UPath mocking: Testing write operations without filesystem dependency
🧪 Data validation: Ensuring proper format handling and error cases
"""

import pytest
import json
import yaml
import pandas as pd
from io import BytesIO
from unittest.mock import patch, MagicMock, mock_open

from src.main.file_io.json import JsonFileIO
from src.main.file_io.text import TextFileIO
from src.main.file_io.yaml import YamlFileIO
from src.main.file_io.csv import CSVFileIO

pytestmark = pytest.mark.unit


class TestJsonFileIO:
    """Test JsonFileIO read and write operations."""
    
    @pytest.fixture
    def sample_json_data(self):
        """Sample JSON data for testing."""
        return {
            "string": "hello world",
            "number": 42,
            "float": 3.14159,
            "boolean": True,
            "null": None,
            "array": [1, 2, 3, "four"],
            "object": {
                "nested": "value",
                "count": 10
            }
        }
    
    @pytest.fixture
    def json_bytes(self, sample_json_data):
        """BytesIO object containing JSON data."""
        json_string = json.dumps(sample_json_data)
        return BytesIO(json_string.encode())
    
    def test_read_valid_json(self, json_bytes, sample_json_data):
        """Test reading valid JSON from BytesIO."""
        result = JsonFileIO._read(json_bytes)
        assert result == sample_json_data
    
    def test_read_empty_json_object(self):
        """Test reading empty JSON object."""
        empty_json = BytesIO(b'{}')
        result = JsonFileIO._read(empty_json)
        assert result == {}
    
    def test_read_json_array(self):
        """Test reading JSON array."""
        json_array = [1, 2, 3, "test", {"key": "value"}]
        json_bytes = BytesIO(json.dumps(json_array).encode())
        result = JsonFileIO._read(json_bytes)
        assert result == json_array
    
    def test_read_invalid_json_raises_error(self):
        """Test that invalid JSON raises JSONDecodeError."""
        invalid_json = BytesIO(b'{"invalid": json}')  # Missing quotes around json
        
        with pytest.raises(json.JSONDecodeError):
            JsonFileIO._read(invalid_json)
    
    def test_read_empty_bytes_raises_error(self):
        """Test that empty bytes raises JSONDecodeError."""
        empty_bytes = BytesIO(b'')
        
        with pytest.raises(json.JSONDecodeError):
            JsonFileIO._read(empty_bytes)
    
    @patch('builtins.open', new_callable=mock_open)
    def test_write_json_data(self, mock_file, sample_json_data):
        """Test writing JSON data to file."""
        from upath import UPath
        
        # Create mock UPath object
        mock_upath = MagicMock(spec=UPath)
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file.return_value
        
        JsonFileIO._write(mock_upath, sample_json_data)
        
        # Verify file was opened in write mode
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'w')
        
        # Verify JSON was written (we can't easily check exact format due to JSON serialization)
        mock_file.return_value.write.assert_called()
    
    def test_write_with_custom_mode(self, sample_json_data):
        """Test writing JSON with custom file mode."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        JsonFileIO._write(mock_upath, sample_json_data, mode='a')
        
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'a')
    
    def test_write_with_json_kwargs(self, sample_json_data):
        """Test writing JSON with additional kwargs."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        JsonFileIO._write(mock_upath, sample_json_data, indent=2, sort_keys=True)
        
        # The kwargs should be passed to json.dump
        # We can't easily verify this without more complex mocking,
        # but we can ensure the method doesn't raise an error
        mock_upath.fs.open.assert_called_once()


class TestTextFileIO:
    """Test TextFileIO read and write operations."""
    
    @pytest.fixture
    def sample_text(self):
        """Sample text data for testing."""
        return "Hello, World!\nThis is a test file\nwith multiple lines."
    
    @pytest.fixture
    def text_bytes(self, sample_text):
        """BytesIO object containing text data."""
        return BytesIO(sample_text.encode())
    
    def test_read_text_from_bytes(self, text_bytes, sample_text):
        """Test reading text from BytesIO."""
        result = TextFileIO._read(text_bytes)
        assert result == sample_text
        assert isinstance(result, str)
    
    def test_read_empty_text(self):
        """Test reading empty text."""
        empty_bytes = BytesIO(b'')
        result = TextFileIO._read(empty_bytes)
        assert result == ""
    
    def test_read_unicode_text(self):
        """Test reading text with unicode characters."""
        unicode_text = "Hello 🌍! Testing unicode: αβγ, 中文, 🚀"
        unicode_bytes = BytesIO(unicode_text.encode('utf-8'))
        result = TextFileIO._read(unicode_bytes)
        assert result == unicode_text
    
    def test_read_text_with_special_chars(self):
        """Test reading text with special characters."""
        special_text = "Line 1\nLine 2\r\nLine 3\tTabbed\nLine with \"quotes\""
        special_bytes = BytesIO(special_text.encode('utf-8'))
        result = TextFileIO._read(special_bytes)
        assert result == special_text
    
    def test_write_text_to_file(self, sample_text):
        """Test writing text to file."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        TextFileIO._write(mock_upath, sample_text)
        
        # Verify file was opened in write mode
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'w')
        
        # Verify text was written
        mock_file.write.assert_called_once_with(sample_text)
    
    def test_write_with_custom_mode(self, sample_text):
        """Test writing text with custom file mode."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        TextFileIO._write(mock_upath, sample_text, mode='a')
        
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'a')
    
    def test_write_empty_text(self):
        """Test writing empty text."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        TextFileIO._write(mock_upath, "")
        
        mock_file.write.assert_called_once_with("")
    
    def test_write_unicode_text(self):
        """Test writing unicode text."""
        unicode_text = "Unicode test: 🌟 αβγ 中文"
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        TextFileIO._write(mock_upath, unicode_text)
        
        mock_file.write.assert_called_once_with(unicode_text)


class TestYamlFileIO:
    """Test YamlFileIO read and write operations."""
    
    @pytest.fixture
    def sample_yaml_data(self):
        """Sample YAML data for testing."""
        return {
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
    
    @pytest.fixture
    def yaml_bytes(self, sample_yaml_data):
        """BytesIO object containing YAML data."""
        yaml_string = yaml.dump(sample_yaml_data)
        return BytesIO(yaml_string.encode())
    
    def test_read_valid_yaml(self, yaml_bytes, sample_yaml_data):
        """Test reading valid YAML from BytesIO."""
        result = YamlFileIO._read(yaml_bytes)
        assert result == sample_yaml_data
    
    def test_read_empty_yaml_document(self):
        """Test reading empty YAML document."""
        empty_yaml = BytesIO(b'')
        result = YamlFileIO._read(empty_yaml)
        assert result is None  # YAML empty document returns None
    
    def test_read_yaml_with_lists_and_dicts(self):
        """Test reading YAML with complex structures."""
        complex_yaml = """
        servers:
          - name: web1
            ip: 192.168.1.10
            services: [http, https]
          - name: web2  
            ip: 192.168.1.11
            services: [http]
        config:
          timeout: 30
          retries: 3
        """
        yaml_bytes = BytesIO(complex_yaml.encode())
        result = YamlFileIO._read(yaml_bytes)
        
        assert "servers" in result
        assert "config" in result
        assert len(result["servers"]) == 2
        assert result["servers"][0]["name"] == "web1"
        assert result["config"]["timeout"] == 30
    
    def test_read_invalid_yaml_raises_error(self):
        """Test that invalid YAML raises YAMLError."""
        invalid_yaml = BytesIO(b'invalid: yaml: content: [unclosed')
        
        with pytest.raises(yaml.YAMLError):
            YamlFileIO._read(invalid_yaml)
    
    def test_write_yaml_data(self, sample_yaml_data):
        """Test writing YAML data to file."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        YamlFileIO._write(mock_upath, sample_yaml_data)
        
        # Verify file was opened in write mode
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'w')
        
        # Verify YAML was written (we can't easily check the exact content 
        # due to YAML formatting variations)
        mock_file.write.assert_called()
    
    def test_write_with_custom_mode(self, sample_yaml_data):
        """Test writing YAML with custom file mode."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        YamlFileIO._write(mock_upath, sample_yaml_data, mode='a')
        
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'a')
    
    def test_write_with_yaml_kwargs(self, sample_yaml_data):
        """Test writing YAML with additional kwargs."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        YamlFileIO._write(mock_upath, sample_yaml_data, default_flow_style=False)
        
        # Should not raise an error
        mock_upath.fs.open.assert_called_once()


class TestCSVFileIO:
    """Test CSVFileIO read and write operations."""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for CSV testing."""
        return pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "city": ["NYC", "SF", "CHI"],
            "salary": [50000, 60000, 70000]
        })
    
    @pytest.fixture
    def csv_bytes(self, sample_dataframe):
        """BytesIO object containing CSV data."""
        csv_string = sample_dataframe.to_csv(index=False)
        return BytesIO(csv_string.encode())
    
    def test_read_csv_to_dataframe(self, csv_bytes, sample_dataframe):
        """Test reading CSV from BytesIO into DataFrame."""
        result = CSVFileIO._read(csv_bytes)
        
        assert isinstance(result, pd.DataFrame)
        assert result.shape == sample_dataframe.shape
        assert list(result.columns) == list(sample_dataframe.columns)
        
        # Check data content (allowing for minor type differences)
        pd.testing.assert_frame_equal(result, sample_dataframe)
    
    def test_read_empty_csv(self):
        """Test reading empty CSV file."""
        empty_csv = BytesIO(b'')
        result = CSVFileIO._read(empty_csv)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_read_csv_with_headers_only(self):
        """Test reading CSV with only headers."""
        headers_only = BytesIO(b'name,age,city\n')
        result = CSVFileIO._read(headers_only)
        
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['name', 'age', 'city']
        assert len(result) == 0
    
    def test_read_csv_with_custom_separator(self):
        """Test reading CSV with custom separator."""
        pipe_separated = BytesIO(b'name|age|city\nAlice|25|NYC\nBob|30|SF')
        result = CSVFileIO._read(pipe_separated, sep='|')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0]['name'] == 'Alice'
        assert result.iloc[0]['age'] == 25
    
    def test_write_dataframe_to_csv(self, sample_dataframe):
        """Test writing DataFrame to CSV file."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        CSVFileIO._write(mock_upath, sample_dataframe)
        
        # Verify file was opened in write mode
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'w', newline='')
        
        # Verify DataFrame was written
        mock_file.write.assert_called()
    
    def test_write_with_custom_mode(self, sample_dataframe):
        """Test writing CSV with custom file mode."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        CSVFileIO._write(mock_upath, sample_dataframe, mode='a')
        
        mock_upath.fs.open.assert_called_once_with(mock_upath.path, 'a', newline='')
    
    def test_write_with_csv_kwargs(self, sample_dataframe):
        """Test writing CSV with additional kwargs."""
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        CSVFileIO._write(mock_upath, sample_dataframe, index=False, sep='|')
        
        # Should not raise an error
        mock_upath.fs.open.assert_called_once()
    
    def test_write_empty_dataframe(self):
        """Test writing empty DataFrame."""
        empty_df = pd.DataFrame()
        mock_upath = MagicMock()
        mock_file = MagicMock()
        mock_upath.fs.open.return_value.__enter__.return_value = mock_file
        
        CSVFileIO._write(mock_upath, empty_df)
        
        mock_upath.fs.open.assert_called_once()
        mock_file.write.assert_called()


class TestFormatClassErrorHandling:
    """Test error handling for specific format classes."""
    
    def test_json_read_with_malformed_data(self):
        """Test JSON reading with malformed data."""
        malformed_json = BytesIO(b'{"unclosed": "object"')
        
        with pytest.raises(json.JSONDecodeError):
            JsonFileIO._read(malformed_json)
    
    def test_yaml_read_with_malformed_data(self):
        """Test YAML reading with malformed data."""
        malformed_yaml = BytesIO(b'invalid: yaml:\n  bad_indentation\nwrong_level')
        
        with pytest.raises(yaml.YAMLError):
            YamlFileIO._read(malformed_yaml)
    
    def test_csv_read_with_malformed_data(self):
        """Test CSV reading with malformed data."""
        # CSV is quite forgiving, but we can test with binary data
        binary_data = BytesIO(b'\xff\xfe\x00\x00invalid\x01\x02')
        
        # This might not always raise an error depending on pandas version
        # but it should handle gracefully
        try:
            result = CSVFileIO._read(binary_data)
            # If no error, result should still be a DataFrame
            assert isinstance(result, pd.DataFrame)
        except (UnicodeDecodeError, pd.errors.EmptyDataError):
            # These are expected errors for binary data
            pass
    
    def test_text_read_with_binary_data(self):
        """Test text reading with binary data that can't be decoded."""
        binary_data = BytesIO(b'\xff\xfe\x00\x00\x01\x02\x03\x04')
        
        with pytest.raises(UnicodeDecodeError):
            TextFileIO._read(binary_data)


# ========================================================================================
# PYTEST LEARNING TIP 💡
# When testing file format classes, focus on:
# 1. Valid data handling (happy path)
# 2. Edge cases (empty data, minimal data)
# 3. Error conditions (malformed data, encoding issues)
# 4. Format-specific features (CSV separators, JSON/YAML structures)
# 5. Integration with the broader system (UPath, file modes)
# ========================================================================================
