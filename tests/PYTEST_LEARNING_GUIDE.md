# Pytest Unit Testing - Comprehensive Learning Guide

## 🎯 What You'll Learn

By the end of this guide, you'll master pytest fundamentals that apply to any Python project:
- **Writing effective unit tests** with pytest framework
- **Using fixtures, mocking, and parametrization** for robust testing
- **Testing patterns** for classes, functions, and modules
- **Best practices** for maintainable test suites
- **Advanced techniques** like file I/O testing and error handling

*Note: Examples use LogManager from this project, but patterns apply to any Python codebase.*

## 📚 Learning Journey

### 🔰 Step 1: The Basics

#### What Makes a Good Test?

A good unit test is **FIRST**:
- **Fast** - Runs in milliseconds
- **Independent** - Doesn't depend on other tests
- **Repeatable** - Same result every time
- **Self-validating** - Clear pass/fail result
- **Timely** - Written close to the code

#### The AAA Pattern

Every test follows this structure:
```python
def test_something():
    # Arrange - Set up test conditions
    # Act - Do the thing you're testing  
    # Assert - Check that it worked
```

#### Your First Test

```python
# Required imports for any Python project
import pytest
from unittest.mock import patch, MagicMock

# Import your own modules to test
from your_module import YourClass  # Example: from logmanager import LogManager

def test_class_creates_successfully():
    """Test that your class can be created with default settings."""
    # Arrange - (no setup needed)
    
    # Act - Create your object
    obj = YourClass()  # Example: lm = LogManager()
    
    # Assert - Verify it was created correctly
    assert obj is not None
    # Add specific assertions for your class
    # Example: assert lm._config_path == LogManager.DEFAULT_CONFIG_PATH
```

**Key Points:**
- Test names should describe what you're testing
- Use simple `assert` statements
- Add docstrings to explain the test purpose
- Import your own modules to test them

---

### 🧪 Step 2: Fixtures - Reusable Test Setup

#### What Are Fixtures?

Fixtures are "test ingredients" that:
- **Set up** test data before tests run
- **Clean up** automatically after tests finish
- **Can be reused** by multiple tests
- **Prevent code duplication**

#### Basic Fixture Example

```python
@pytest.fixture
def sample_config():
    """Provide test configuration data for any config-based class."""
    # Return configuration data relevant to your project
    return {
        'setting1': 'value1',
        'setting2': 42,
        'features': ['feature_a', 'feature_b']
    }
    # LogManager example:
    # return {
    #     'formats': {'simple': '{message}'},
    #     'handlers': {'console': {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'}},
    #     'loggers': {'test_logger': [{'handler': 'console', 'level': 'INFO'}]}
    # }

def test_config_loading(sample_config, tmp_path):
    """Test that your class loads configuration correctly."""
    # Arrange - Create temporary config file (if your class uses files)
    import yaml  # or json, depending on your config format
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(yaml.dump(sample_config))
    
    # Act - Load your class with config
    with patch('your_module.external_dependency'):  # Mock external dependencies
        obj = YourClass(config_path=str(config_file))
        # LogManager example: lm = LogManager(config_path=str(config_file))
    
    # Assert - Verify config was loaded
    assert obj.config == sample_config
    # LogManager example: assert lm.config == sample_config
```

#### How Fixtures Work

1. Test requests fixture by name (as parameter)
2. Pytest runs fixture function first  
3. Fixture return value is passed to test
4. Cleanup happens automatically

#### Fixture Scopes

```python
@pytest.fixture                    # Function scope (default) - fresh per test
@pytest.fixture(scope="class")     # Class scope - shared by test class
@pytest.fixture(scope="module")    # Module scope - shared by test file
@pytest.fixture(scope="session")   # Session scope - shared by all tests
```

#### Fixtures with Cleanup

```python
@pytest.fixture
def temp_resource():
    """Create temporary resource with automatic cleanup."""
    # Setup - Create resource (file, connection, etc.)
    resource = "temp_data.txt"  # or database connection, etc.
    
    yield resource  # This gets passed to the test
    
    # Cleanup runs after test (even if test fails)
    import os
    if os.path.exists(resource):
        os.remove(resource)
    # Or close database connections, cleanup state, etc.
    
# LogManager example:
@pytest.fixture
def temp_log_file():
    """Create temporary log file with automatic cleanup."""
    log_file = "test_app.log"
    
    yield log_file  # This gets passed to the test
    
    # Cleanup runs after test (even if test fails)
    if os.path.exists(log_file):
        os.remove(log_file)
```

#### Built-in Fixtures You'll Use

- `tmp_path` - Temporary directory for each test
- `monkeypatch` - Safely modify objects/environment  
- `caplog` - Capture log output for testing

---

### 🎭 Step 3: Mocking - Avoiding Side Effects

#### Why Mock?

When testing, you want to avoid **side effects**:
- Creating actual files
- Printing to console
- Making network calls
- Slow operations

#### What Mocking Does

```python
# Without mocking - BAD for tests (creates side effects):
obj = MyClass()  
# ↳ Might create real files
# ↳ Might make network calls
# ↳ Might print to console
# ↳ Slow and unpredictable

# With mocking - GOOD for tests (fast and isolated):
with patch('mymodule.external_dependency') as mock_dep:
    obj = MyClass()
    # ↳ External operations are "faked"
    # ↳ No side effects, fast and predictable

# LogManager example:
# with patch('logmanager.logger') as mock_logger:
#     lm = LogManager()
#     # ↳ Logger operations are "faked"
#     # ↳ No files created, fast and clean
```

#### Basic Mocking Example

```python
@patch('mymodule.external_service')
def test_method_calls_service(mock_service):
    """Test that your method calls external service correctly."""
    # Arrange - Set up mock behavior
    mock_service.process.return_value = 'mocked_result'
    obj = MyClass()
    
    # Act - Call your method
    result = obj.do_something_with_service('test_input')
    
    # Assert - Verify the mock was called correctly
    mock_service.process.assert_called_once_with('test_input')
    assert result == 'mocked_result'

# LogManager example:
@patch('logmanager.logger')
def test_handler_creation(mock_logger):
    """Test that add_handler calls loguru correctly."""
    # Arrange
    mock_logger.add.return_value = 'fake_handler_id'
    lm = LogManager()
    
    # Act
    lm.add_handler('test', {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'})
    
    # Assert - Verify the mock was called correctly
    mock_logger.add.assert_called_once()
    call_args = mock_logger.add.call_args[1]
    assert call_args['level'] == 'INFO'
```

#### Important: Where to Patch

**Rule:** Patch where your code **imports** the object, not where it's **defined**.

```python
# In your_module.py:
from external_lib import some_function  # Creates your_module.some_function

# So patch:
patch('your_module.some_function')  # ✅ Correct
# NOT:
patch('external_lib.some_function') # ❌ Wrong - your code won't see this

# LogManager example:
# In logmanager.py:
# from loguru import logger  # Creates logmanager.logger
# So patch:
# patch('logmanager.logger')  # ✅ Correct
# NOT:
# patch('loguru.logger')      # ❌ Wrong - your code won't see this
```

---

### 🔄 Step 4: Parametrized Tests - Test Multiple Scenarios

#### What Are Parametrized Tests?

Run the same test with different inputs to:
- **Reduce code duplication** 
- **Test multiple scenarios** efficiently
- **Get clear reporting** for each case

#### Basic Example

```python
@pytest.mark.parametrize("input_value,expected_output", [
    ("lowercase", "LOWERCASE"),   # transformation case
    ("UPPERCASE", "UPPERCASE"),   # already correct
    ("MiXeD", "MIXED"),          # mixed case
    ("", ""),                    # edge case: empty string
])
def test_string_normalization(input_value, expected_output):
    """Test that strings are normalized to uppercase."""
    obj = MyStringProcessor()
    result = obj.normalize(input_value)
    assert result == expected_output

# LogManager example:
@pytest.mark.parametrize("level_input,expected_output", [
    ("debug", "DEBUG"),      # lowercase to uppercase
    ("INFO", "INFO"),        # already uppercase
    ("Warning", "WARNING"),  # mixed case
])
def test_level_normalization(level_input, expected_output):
    """Test that log levels are normalized to uppercase."""
    lm = LogManager()
    
    with patch('logmanager.logger') as mock_logger:
        lm.add_handler('test', {'sink': 'sys.stdout', 'level': level_input})
        call_args = mock_logger.add.call_args[1]
        assert call_args['level'] == expected_output
```

This creates separate tests for each parameter combination:
- `test_string_normalization[lowercase-LOWERCASE]`
- `test_string_normalization[UPPERCASE-UPPERCASE]`
- etc.

#### Testing Error Cases

```python
@pytest.mark.parametrize("invalid_input", [
    None,                           # null input
    {},                            # empty dict (if expecting specific keys)
    {"missing": "required_key"},   # missing required fields
    {"invalid": "data_type"},      # wrong data type
])
def test_invalid_inputs_raise_errors(invalid_input):
    """Test that invalid inputs raise appropriate errors."""
    obj = MyClass()
    
    with pytest.raises((ValueError, KeyError, TypeError)):
        obj.process(invalid_input)

# LogManager example:
@pytest.mark.parametrize("invalid_config", [
    {},                                    # No keys
    {'sink': 'sys.stdout'},               # Missing level
    {'level': 'INFO'},                    # Missing sink
])
def test_invalid_configs_raise_errors(invalid_config):
    """Test that invalid configurations raise errors."""
    lm = LogManager()
    
    with pytest.raises(KeyError):
        lm.add_handler('test', invalid_config)
```

---

### 🧩 Step 5: Testing Real Functionality

#### Testing State Management

```python
def test_add_item_stores_correctly():
    """Test that items are stored in the internal data structure."""
    # Arrange
    obj = MyContainer()
    item_data = {'name': 'test_item', 'value': 42}
    
    # Act
    obj.add_item('test_key', item_data)
    
    # Assert
    assert 'test_key' in obj._internal_storage
    assert obj._internal_storage['test_key'] == item_data

# LogManager example:
def test_add_handler_stores_correctly():
    """Test that handlers are stored in the internal map."""
    # Arrange
    lm = LogManager()
    config = {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'}
    
    # Act
    with patch('logmanager.logger'):
        lm.add_handler('test_handler', config)
    
    # Assert
    assert 'test_handler' in lm._handlers_map
```

#### Testing Error Handling

```python
def test_duplicate_names_not_allowed():
    """Test that duplicate names raise errors."""
    obj = MyContainer()
    item1 = {'type': 'first', 'value': 1}
    item2 = {'type': 'second', 'value': 2}
    
    obj.add_item('same_name', item1)
    
    # Try to add another item with same name
    with pytest.raises(ValueError, match="already exists"):
        obj.add_item('same_name', item2)

# LogManager example:
def test_duplicate_handler_names_not_allowed():
    """Test that duplicate handler names raise errors."""
    lm = LogManager()
    config1 = {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'}
    config2 = {'sink': 'sys.stderr', 'format': 'detailed', 'level': 'DEBUG'}
    
    with patch('logmanager.logger'):
        lm.add_handler('same_name', config1)
        
        # Try to add another handler with same name
        with pytest.raises(AssertionError, match="already exists"):
            lm.add_handler('same_name', config2)
```

#### Understanding `pytest.raises()`

```python
with pytest.raises(ValueError, match="specific error message"):
    obj.method_that_should_fail(bad_input)
```

This means:
- "I expect a `ValueError` to be raised"
- "The error message should contain 'specific error message'"
- "If no error occurs, the test fails"
- "If wrong error type occurs, the test fails"

**Common Error Types to Test:**
- `ValueError` - Invalid values or arguments
- `TypeError` - Wrong data types
- `KeyError` - Missing dictionary keys
- `FileNotFoundError` - Missing files
- `AttributeError` - Missing attributes/methods

---

### 🔗 Step 6: Integration Testing

Test complete workflows that match real usage:

```python
def test_complete_workflow():
    """Test the full workflow from initialization to final operation."""
    # Mock external dependencies to avoid side effects
    with patch('your_module.external_service') as mock_service:
        mock_service.connect.return_value = True
        mock_service.process.return_value = MagicMock()
        
        # Complete workflow
        obj = YourClass(config_path="./config.yaml")
        result = obj.initialize()
        
        # Perform main operations
        obj.add_component("component_a", {"setting": "value"})
        final_result = obj.execute()
        
        # Verify everything worked
        assert result is not None
        assert "component_a" in obj._components
        assert final_result.success is True

# LogManager example:
def test_complete_logging_workflow():
    """Test the full workflow from config to logging."""
    with patch('logmanager.logger') as mock_logger:
        mock_logger.add.return_value = 'mock_id'
        mock_logger.bind.return_value = MagicMock()
        
        # Complete workflow
        lm = LogManager(config_path="./example_config.yaml")
        logger_a = lm.get_logger("logger_a")
        
        # Add new handler dynamically
        lm.add_handler("console_fire", {
            "sink": "sys.stdout", 
            "level": "info",
            "format": "🔥 {message}"
        })
        
        # Verify everything worked
        assert logger_a is not None
        assert "console_fire" in lm._handlers_map
```

---

## 🔧 Running Tests

### Basic Commands
```bash
# Install testing tools
pip install pytest pytest-cov

# Run all tests with details
pytest tests/ -v

# Run specific test file
pytest tests/test_your_module.py -v

# Run specific test function
pytest tests/test_your_module.py::test_function_name -v

# Run with coverage report
pytest --cov=your_module --cov-report=html

# Stop on first failure
pytest -x

# Show local variables on failure
pytest -l

# Run tests matching pattern
pytest -k "test_config" -v
```

### Debugging Failed Tests
```bash
# Drop into debugger on failure
pytest --pdb

# Show more output
pytest -s

# Run only failed tests from last run
pytest --lf
```

---

## 🛠️ Practical Exercises

### Exercise 1: Write Your First Test
1. Choose a simple class or function in your project
2. Write a basic test that creates an instance and checks it exists
3. Add assertions for basic properties or return values

### Exercise 2: Create a Fixture
1. Identify test data that multiple tests could use
2. Create a fixture that provides this data
3. Use it in multiple tests to avoid duplication

### Exercise 3: Use Parametrized Testing
1. Find a function that should handle multiple input types
2. Create parametrized tests with different valid inputs
3. Add test cases for invalid inputs that should raise errors

### Exercise 4: Test Error Conditions
1. Identify methods that should validate input
2. Test what happens with invalid parameters
3. Verify appropriate exceptions are raised

### Exercise 5: Mock External Dependencies
1. Find code that calls external services/libraries
2. Mock the external dependencies
3. Test your code's behavior when mocks return different values

---

## 📋 Testing Checklist

For each class/function in your project, test:
- [ ] **Happy path** - Works with valid inputs
- [ ] **Edge cases** - Boundary conditions (empty lists, None values, etc.)
- [ ] **Error cases** - Invalid inputs raise appropriate errors
- [ ] **State changes** - Internal state updates correctly
- [ ] **Side effects** - External calls happen as expected
- [ ] **Integration** - Works correctly with other components

---

## 💡 Key Takeaways

- **Start simple** - Begin with basic functionality tests
- **Use AAA pattern** - Arrange, Act, Assert for clear test structure
- **Mock external dependencies** - Keep tests fast and isolated
- **Test error cases** - Don't just test happy paths
- **Use descriptive names** - Tests should tell a story about expected behavior
- **Keep tests independent** - Each test should run alone successfully
- **Organize with fixtures** - Reuse setup code across multiple tests
- **Parametrize similar tests** - Avoid code duplication for multiple inputs

**Remember:** Good tests are an investment in your codebase. They catch bugs early, enable confident refactoring, and document how your code should behave. These patterns work for any Python project, not just LogManager!

---

## 📁 Advanced Pattern: Testing File I/O Operations

### FileIO Testing Challenges
File I/O testing requires special considerations:
- **Temporary files** - Don't leave test files lying around
- **Platform differences** - Paths and permissions vary
- **Error simulation** - Test disk full, permission denied scenarios
- **Performance** - Large files can slow down tests

### Essential FileIO Testing Fixtures

```python
@pytest.fixture
def temp_directory(tmpdir):
    """Clean temporary directory for each test."""
    return str(tmpdir)

@pytest.fixture
def sample_json_data():
    """Reusable test data."""
    return {"name": "test", "value": 123, "active": True}

@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame for CSV/Parquet testing."""
    return pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
```

### FileIO Testing Patterns

#### 1. Round-Trip Testing (Write → Read → Verify)
```python
def test_json_round_trip(temp_directory, sample_json_data):
    """Test writing and reading JSON preserves data."""
    # Arrange
    json_file = os.path.join(temp_directory, "test.json")
    
    # Act
    FileIOInterface.fwrite(json_file, sample_json_data)
    result = FileIOInterface.fread(json_file)
    
    # Assert
    assert result == sample_json_data
```

#### 2. Format-Specific Testing with Parametrization
```python
@pytest.mark.parametrize("extension,data_type,sample", [
    ("json", dict, {"test": "data"}),
    ("txt", str, "test content"),
    ("yaml", dict, {"yaml": "config"}),
])
def test_multiple_formats(temp_directory, extension, data_type, sample):
    """Test multiple file formats with same logic."""
    file_path = os.path.join(temp_directory, f"test.{extension}")
    
    FileIOInterface.fwrite(file_path, sample)
    result = FileIOInterface.fread(file_path)
    
    assert result == sample
    assert isinstance(result, data_type)
```

#### 3. Error Condition Testing
```python
def test_read_nonexistent_file():
    """Test reading missing file raises appropriate error."""
    with pytest.raises(FileNotFoundError):
        FileIOInterface.fread("nonexistent.json")

def test_write_invalid_data_type():
    """Test type validation for file formats."""
    with pytest.raises(TypeError, match="requires a pandas DataFrame"):
        FileIOInterface.fwrite("test.csv", "not a dataframe")
```

#### 4. Mocking Filesystem Operations
```python
@patch('src.main.file_io.FileIOInterface._instantiate')
def test_disk_full_simulation(mock_instantiate):
    """Test handling of disk space errors."""
    mock_fileio = MagicMock()
    mock_fileio._fwrite.side_effect = OSError("No space left")
    mock_instantiate.return_value = mock_fileio
    
    with pytest.raises(OSError, match="No space left"):
        FileIOInterface.fwrite("test.json", {"data": "test"})
```

#### 5. Integration Testing with Real Files
```python
@pytest.mark.integration
def test_data_pipeline(temp_directory):
    """Test complete data processing workflow."""
    # Create input data
    input_data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
    csv_path = os.path.join(temp_directory, "input.csv")
    FileIOInterface.fwrite(csv_path, input_data)
    
    # Process data
    loaded_data = FileIOInterface.fread(csv_path)
    filtered_data = loaded_data[loaded_data["age"] > 26]
    
    # Save processed data
    output_path = os.path.join(temp_directory, "output.json")
    FileIOInterface.fwrite(output_path, filtered_data.to_dict("records"))
    
    # Verify results
    result = FileIOInterface.fread(output_path)
    assert len(result) == 1
    assert result[0]["name"] == "Bob"
```

### FileIO Test Organization

```
tests/fileio/
├── conftest.py                # Shared fixtures
├── test_fileio_interface.py   # Main interface tests
├── test_format_classes.py     # Format-specific tests
└── test_integration.py        # End-to-end scenarios
```

### Key FileIO Testing Tips

✅ **Do:**
- Use `tmpdir` fixture for temporary files
- Test both success and failure paths
- Use parametrization for multiple formats
- Mock filesystem for unit tests, use real files for integration
- Test data type validation
- Include performance tests for large files

❌ **Don't:**
- Leave test files in filesystem after tests
- Use hardcoded paths
- Test only happy paths
- Ignore platform differences
- Make tests dependent on external resources

### FileIO Test Markers

Organize tests with markers:
```python
@pytest.mark.unit           # Fast, isolated tests
@pytest.mark.integration    # Real filesystem tests
@pytest.mark.performance    # Speed/memory tests
@pytest.mark.error_handling # Error condition tests
```

Run specific test types:
```bash
pytest tests/fileio/ -m "unit"              # Fast tests only
pytest tests/fileio/ -m "not performance"   # Skip slow tests
pytest tests/fileio/ -m "integration"       # Real file tests
```

---

## 🚀 Next Steps

Once you've mastered these basics with any Python project:

1. **Test-Driven Development (TDD)** - Write tests before code
2. **Property-Based Testing** - Use Hypothesis for edge case discovery
3. **Advanced Mocking** - Learn context managers, side effects, and spec
4. **Mutation Testing** - Test your tests with tools like mutmut
5. **CI/CD Integration** - Automate testing in your development pipeline
6. **Performance Testing** - Profile and benchmark critical code paths

**Project-Specific Advanced Patterns:**
- **File I/O Testing** - For data processing applications (see FileIO section above)
- **Database Testing** - For applications with persistent storage
- **API Testing** - For web services and REST APIs
- **Async Testing** - For asyncio and concurrent code

The patterns in this guide apply to any Python project - web apps, data science, CLI tools, libraries, and more. Adapt the examples to your specific domain and start building robust test suites!
