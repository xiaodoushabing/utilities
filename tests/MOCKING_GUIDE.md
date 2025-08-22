# Python Mocking Guide for Testing

A comprehensive guide to understanding different mocking patterns used in our test suite. This document explains when and how to use different `unittest.mock` techniques.

## Table of Contents
- [Overview](#overview)
- [Mocking Patterns](#mocking-patterns)
- [Pytest Fixtures](#pytest-fixtures)
- [Quick Reference](#quick-reference)
- [Common Patterns in Our Codebase](#common-patterns-in-our-codebase)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

Mocking allows you to replace parts of your system under test with mock objects and make assertions about how they're used. The key principle: **patch where the object is imported, not where it's defined**.

## Mocking Patterns

### 1. `patch.object()` - Mock Methods on Classes

**Use when**: You want to mock a specific method on a class you're testing.

```python
# Pattern
with patch.object(ClassName, 'method_name') as mock_method:
    mock_method.return_value = "fake_result"
    # Your test code here

# Example from our conftest.py
with patch.object(LogManager, '_discover_files_to_copy') as mock:
    yield mock
```

**Real-world example**:
```python
class Calculator:
    def add(self, a, b):
        return a + b
    
    def complex_calculation(self):
        return self.add(5, 3)  # We want to mock this call

# In test
with patch.object(Calculator, 'add', return_value=999) as mock_add:
    calc = Calculator()
    result = calc.complex_calculation()  # Returns 999, not 8!
    mock_add.assert_called_with(5, 3)
```

**When to use**:
- Testing internal method calls
- Avoiding expensive operations in unit tests
- Isolating the method under test

### 2. `patch()` with Import Path - Mock External Dependencies

**Use when**: You need to mock external libraries or modules that are imported.

```python
# Pattern
with patch('module.where.imported.object') as mock_obj:
    mock_obj.return_value = "fake_result"
    # Your test code here

# Example from our conftest.py
with patch('utilities.logger.logger') as mock:
    mock.add.return_value = '123'
    yield mock
```

**Real-world example**:
```python
# In your_module.py
import requests

def fetch_user_data(user_id):
    response = requests.get(f'https://api.example.com/users/{user_id}')
    return response.json()

# In test - patch where it's imported!
with patch('your_module.requests.get') as mock_get:
    mock_response = MagicMock()
    mock_response.json.return_value = {'id': 123, 'name': 'Test User'}
    mock_get.return_value = mock_response
    
    result = fetch_user_data(123)
    assert result['name'] == 'Test User'
    mock_get.assert_called_with('https://api.example.com/users/123')
```

**When to use**:
- Mocking HTTP requests (requests, urllib)
- Mocking database connections
- Mocking external APIs

### 3. `patch()` with Built-ins - Mock System Functions

**Use when**: You need to mock Python's built-in functions like `open`, `print`, `input`.

```python
# Pattern
with patch('builtins.function_name', mock_function) as mock_func:
    # Your test code here

# Example from our conftest.py
with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
    return LogManager()
```

**Real-world example**:
```python
# Your code
import json

def save_config(data):
    with open('config.json', 'w') as f:
        json.dump(data, f)

def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

# In test
import json
from unittest.mock import patch, mock_open

test_data = {'setting': 'value'}

# Mock file writing
with patch('builtins.open', mock_open()) as mock_file:
    save_config(test_data)
    mock_file.assert_called_with('config.json', 'w')
    # Verify json.dump was called with our data
    handle = mock_file.return_value.__enter__.return_value
    # Note: You could also mock json.dump separately for more control

# Mock file reading
with patch('builtins.open', mock_open(read_data='{"setting": "value"}')):
    config = load_config()
    assert config == {'setting': 'value'}
```

**When to use**:
- Avoiding real file I/O in tests
- Mocking user input
- Controlling print output

### 4. `patch()` with Third-Party Classes - Mock Complex Objects

**Use when**: You need to mock classes from external libraries (threading, datetime, etc.).

```python
# Pattern
with patch('module.where.imported.ClassName') as MockClass:
    mock_instance = MagicMock()
    MockClass.return_value = mock_instance
    # Configure mock_instance behavior

# Example from our conftest.py
with patch('utilities.logger.threading.Event') as mock_event:
    mock_event_instance = MagicMock()
    mock_event.return_value = mock_event_instance
```

#### When You Need Explicit `MagicMock()` vs Auto-Mock

When you use `patch()`, it automatically creates a `MagicMock` object. You have two options for configuring the mock's behavior:

1. **Auto-mock**: Use the automatically created `MockClass.return_value` 
2. **Explicit MagicMock**: Create your own `MagicMock()` instance for custom control

**Auto-mock is sufficient** (simpler):
```python
# patch() creates MockThread as a MagicMock that replaces the Thread class
with patch('utilities.logger.threading.Thread') as MockThread:
    # MockThread.return_value is automatically another MagicMock representing a Thread instance
    # Configure the instance methods directly
    MockThread.return_value.start.return_value = None      # Mock start() method
    MockThread.return_value.is_alive.return_value = True   # Mock is_alive() method
    
    # When code calls: thread = Thread(); thread.start()
    # It actually calls: MockThread(); MockThread.return_value.start()
```
*In practice:*
```python
# Original code that gets mocked:
thread = threading.Thread(target=some_function)  # This calls MockThread()
thread.start()     # This calls MockThread.return_value.start()
thread.is_alive()  # This calls MockThread.return_value.is_alive()
```

**Explicit MagicMock needed** (complex behavior):
```python
# From our conftest.py - when you need custom behavior
with patch('utilities.logger.threading.Thread') as mock_thread:
    # Create your own MagicMock instance for full control
    mock_thread_instance = MagicMock()  # ← Explicit creation for custom logic
    
    def create_mock_thread(*args, **kwargs):
        # Custom configuration based on constructor arguments
        mock_thread_instance.name = kwargs.get('name', 'MockThread')
        mock_thread_instance.daemon = kwargs.get('daemon', False)
        mock_thread_instance.is_alive.return_value = True
        
        # Create stateful behavior - join() changes is_alive() result
        def mock_join(timeout=None):
            mock_thread_instance.is_alive.return_value = False
        
        mock_thread_instance.join.side_effect = mock_join
        return mock_thread_instance  # Return the same instance each time
    
    # Replace the default behavior with custom creation logic
    mock_thread.side_effect = create_mock_thread
    
    # Now when code calls Thread(name='worker'), it uses our custom logic
```

**Use explicit MagicMock when you need**:
- Custom behavior based on constructor arguments
- Stateful behavior (like the join/is_alive interaction above)
- Shared references to the same mock instance
- Complex side effects

**Key Difference Explained**:
- **Auto-mock**: `MockThread.return_value` is automatically a `MagicMock` - simple configuration
- **Explicit MagicMock**: You create `mock_instance = MagicMock()` - full control over behavior

In both cases, `MockThread` itself is a `MagicMock` that replaces the Thread class constructor. The difference is how you configure what gets returned when that constructor is called.

**When to use**:
- Avoiding real threading in tests
- Mocking time-related functions
- Mocking complex external objects

## Pytest Fixtures

Pytest fixtures are reusable test components that provide data, mock objects, or setup/teardown functionality. They're essential for maintaining clean, DRY (Don't Repeat Yourself) test code.

### Understanding Fixtures

**What are fixtures?**: Functions decorated with `@pytest.fixture` that provide test dependencies. They run before tests and can provide return values to test functions.

**Why use fixtures?**: 
- Eliminate code duplication across tests
- Provide consistent test setup
- Automatic cleanup after tests
- Dependency injection for tests

### Basic Fixture Patterns

#### 1. Simple Data Fixtures
```python
@pytest.fixture
def sample_user_data():
    """Provides sample user data for testing."""
    return {
        'id': 123,
        'name': 'Test User',
        'email': 'test@example.com'
    }

# Usage in test
def test_user_creation(sample_user_data):
    user = create_user(sample_user_data)
    assert user.name == 'Test User'
```

#### 2. Setup/Teardown Fixtures
```python
@pytest.fixture
def temp_dir():
    """Creates a temporary directory and cleans up after test."""
    temp_path = tempfile.mkdtemp()
    yield temp_path  # This is where the test runs
    shutil.rmtree(temp_path, ignore_errors=True)  # Cleanup

# Usage in test
def test_file_operations(temp_dir):
    file_path = os.path.join(temp_dir, 'test.txt')
    # Create files in temp_dir
    # No manual cleanup needed!
```

#### 3. Mock Fixtures (Our Main Pattern)
```python
@pytest.fixture
def mock_database():
    """Provides a mocked database connection."""
    with patch('myapp.database.connect') as mock_conn:
        mock_conn.return_value.query.return_value = [{'id': 1}]
        yield mock_conn

# Usage in test
def test_data_retrieval(mock_database):
    result = get_user_data(123)
    mock_database.return_value.query.assert_called_with('SELECT * FROM users WHERE id = 123')
```

### Fixture Scopes

Control how long fixtures live and when they're recreated:

```python
# Function scope (default) - new instance for each test
@pytest.fixture(scope='function')
def fresh_mock():
    with patch('module.function') as mock:
        yield mock

# Class scope - shared across all tests in a class
@pytest.fixture(scope='class')
def shared_database():
    db = setup_test_database()
    yield db
    teardown_test_database(db)

# Module scope - shared across all tests in a file
@pytest.fixture(scope='module')
def expensive_setup():
    # This runs once per test file
    return setup_expensive_resource()

# Session scope - shared across entire test run
@pytest.fixture(scope='session')
def global_config():
    return load_global_test_config()
```

### Fixture Dependencies

Fixtures can depend on other fixtures:

```python
@pytest.fixture
def mock_logger():
    with patch('utilities.logger.logger') as mock:
        mock.add.return_value = '123'
        yield mock

@pytest.fixture
def log_manager(mock_logger, default_config):
    """LogManager fixture depends on mock_logger and default_config."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(default_config))):
        return LogManager()

# Usage - pytest automatically provides dependencies
def test_logging(log_manager):  # Gets mock_logger and default_config automatically
    logger = log_manager.get_logger('test')
    logger.info('test message')
```

### Fixtures from Our Codebase

#### 1. Temporary Directory Fixture
```python
@pytest.fixture
def temp_dir():
    """Creates a safe temporary folder for testing file operations."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)

# Why this pattern?
# - Automatic cleanup prevents test pollution
# - Each test gets a fresh directory
# - Safe place to create test files
```

#### 2. Mock Logger Fixture
```python
@pytest.fixture
def mock_logger():
    """Replaces real Loguru logger with controllable mock."""
    with patch('utilities.logger.logger') as mock:
        mock.add.return_value = '123'
        mock.level.return_value = MagicMock(no=20)
        mock.bind.return_value = MagicMock()
        mock.remove.return_value = None
        yield mock

# Why this pattern?
# - Prevents real logging during tests
# - Allows verification of logging calls
# - Provides consistent logger behavior
```

#### 3. Configuration Fixture
```python
@pytest.fixture
def default_config():
    """Provides minimal test configuration."""
    return {
        'formats': {'simple': '{level} | {message}'},
        'handlers': {
            "handler_file": {
                'sink': '.test.log',
                'format': 'simple',
                'level': 'DEBUG'
            }
        },
        'loggers': {
            "logger_a": [{'handler': 'handler_file', 'level': 'DEBUG'}]
        }
    }

# Why this pattern?
# - Consistent test configuration
# - Easy to modify for specific tests
# - Isolated from real config files
```

#### 4. Complex Mock Fixtures
```python
@pytest.fixture
def mock_thread():
    """Creates realistic mock threading.Thread behavior."""
    with patch('utilities.logger.threading.Thread') as mock_thread:
        mock_thread_instance = MagicMock()
        
        def create_mock_thread(*args, **kwargs):
            mock_thread_instance.name = kwargs.get('name', 'MockThread')
            mock_thread_instance.daemon = kwargs.get('daemon', False)
            mock_thread_instance.is_alive.return_value = True
            
            def mock_join(timeout=None):
                mock_thread_instance.is_alive.return_value = False
            
            mock_thread_instance.join.side_effect = mock_join
            return mock_thread_instance
        
        mock_thread.side_effect = create_mock_thread
        yield mock_thread

# Why this pattern?
# - Provides realistic thread behavior
# - Avoids real threading in tests
# - Allows verification of thread operations
```

### Fixture Best Practices

#### 1. Use Descriptive Names
```python
# ❌ Unclear
@pytest.fixture
def data():
    return {'key': 'value'}

# ✅ Clear
@pytest.fixture
def sample_user_profile():
    return {'user_id': 123, 'name': 'Test User'}
```

#### 2. Keep Fixtures Focused
```python
# ❌ Too many responsibilities
@pytest.fixture
def everything():
    mock_db = patch('db.connect')
    mock_api = patch('api.call')
    temp_dir = tempfile.mkdtemp()
    return mock_db, mock_api, temp_dir

# ✅ Single responsibility
@pytest.fixture
def mock_database():
    with patch('db.connect') as mock:
        yield mock

@pytest.fixture
def mock_api():
    with patch('api.call') as mock:
        yield mock
```

#### 3. Use Appropriate Scopes
```python
# ❌ Expensive setup for every test
@pytest.fixture
def database_connection():
    return create_real_db_connection()  # Slow!

# ✅ Share expensive resources
@pytest.fixture(scope='module')
def database_connection():
    conn = create_real_db_connection()
    yield conn
    conn.close()
```

#### 4. Provide Good Documentation
```python
@pytest.fixture
def mock_hdfs_client():
    """
    Provides a mocked HDFS client for testing file operations.
    
    Returns:
        MagicMock: Mock client with pre-configured methods:
            - upload.return_value = True
            - download.return_value = '/tmp/downloaded'
            - exists.return_value = True
    
    Usage:
        def test_upload(mock_hdfs_client):
            result = upload_to_hdfs('/local/file', '/hdfs/destination')
            mock_hdfs_client.upload.assert_called_once()
    """
    with patch('hdfs.client.Client') as mock_client:
        mock_instance = MagicMock()
        mock_instance.upload.return_value = True
        mock_instance.download.return_value = '/tmp/downloaded'
        mock_instance.exists.return_value = True
        mock_client.return_value = mock_instance
        yield mock_instance
```

### Fixture Composition Patterns

#### 1. Building Complex Fixtures from Simple Ones
```python
@pytest.fixture
def mock_logger():
    with patch('utilities.logger.logger') as mock:
        yield mock

@pytest.fixture
def mock_filesystem():
    with patch('builtins.open', mock_open()) as mock:
        yield mock

@pytest.fixture
def configured_log_manager(mock_logger, mock_filesystem, default_config):
    """Combines multiple fixtures to create a fully configured LogManager."""
    mock_filesystem.return_value.read.return_value = yaml.dump(default_config)
    return LogManager(config_path='test_config.yaml')
```

#### 2. Parametrized Fixtures
```python
@pytest.fixture(params=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
def log_level(request):
    """Provides different log levels for testing."""
    return request.param

def test_logging_at_all_levels(log_level, mock_logger):
    """This test runs 4 times, once for each log level."""
    logger = setup_logger(level=log_level)
    logger.log(log_level, 'test message')
    # Test runs with DEBUG, then INFO, then WARNING, then ERROR
```

#### 3. Conditional Fixtures
```python
@pytest.fixture
def database_type(request):
    """Returns database type based on test marker."""
    if request.node.get_closest_marker('postgres'):
        return 'postgresql'
    elif request.node.get_closest_marker('mysql'):
        return 'mysql'
    else:
        return 'sqlite'

@pytest.mark.postgres
def test_postgres_specific_feature(database_type):
    assert database_type == 'postgresql'
```

### Debugging Fixtures

#### 1. Print Fixture Values
```python
@pytest.fixture
def debug_config(default_config):
    """Debug version that prints config."""
    print(f"Using config: {default_config}")
    return default_config
```

#### 2. Fixture Inspection
```python
# See which fixtures are available
pytest --fixtures test_file.py

# See fixture dependencies
pytest --fixtures-per-test test_file.py::test_function
```

#### 3. Fixture Caching Issues
```python
# Force fixture recreation
@pytest.fixture
def always_fresh():
    """This fixture recreates every time (scope='function' is default)."""
    return create_new_instance()

# Check if fixture is being reused when it shouldn't be
@pytest.fixture(scope='function')  # Explicit scope
def should_be_fresh():
    print("Creating new instance")  # Should print for each test
    return create_new_instance()
```

## Quick Reference

| Pattern | What to Mock | Example Usage |
|---------|--------------|---------------|
| `patch.object(Class, 'method')` | Methods on your own classes | `patch.object(LogManager, '_internal_method')` |
| `patch('module.imported_thing')` | External dependencies | `patch('mymodule.requests.get')` |
| `patch('builtins.function')` | Python built-in functions | `patch('builtins.open')` |
| `patch('module.Class')` | External classes | `patch('mymodule.threading.Thread')` |

### Fixture Reference

| Fixture Type | Scope | Example Usage |
|--------------|-------|---------------|
| `@pytest.fixture` | Function (default) | `def mock_api(): ...` |
| `@pytest.fixture(scope='class')` | Class | Shared across test class |
| `@pytest.fixture(scope='module')` | Module | Shared across test file |
| `@pytest.fixture(scope='session')` | Session | Shared across entire test run |

## Common Patterns in Our Codebase

### File Operations
```python
# Mock file reading with specific content
with patch('builtins.open', mock_open(read_data='fake file content')):
    result = my_function_that_reads_file()

# Mock file writing and verify calls
with patch('builtins.open', mock_open()) as mock_file:
    my_function_that_writes_file()
    mock_file.assert_called_with('expected_filename.txt', 'w')
```

### Threading Operations
```python
# Mock threading.Thread
with patch('utilities.logger.threading.Thread') as mock_thread_class:
    mock_thread_instance = MagicMock()
    mock_thread_class.return_value = mock_thread_instance
    
    # Your test code
    mock_thread_instance.start.assert_called_once()
    mock_thread_instance.join.assert_called_once()
```

### External APIs
```python
# Mock HTTP requests
with patch('mymodule.requests.get') as mock_get:
    mock_response = MagicMock()
    mock_response.json.return_value = {'status': 'success'}
    mock_get.return_value = mock_response
    
    result = my_api_function()
    assert result['status'] == 'success'
```

### Internal Method Calls
```python
# Mock internal methods to isolate tests
with patch.object(MyClass, '_expensive_operation') as mock_op:
    mock_op.return_value = 'fast_fake_result'
    
    obj = MyClass()
    result = obj.public_method()  # Uses mocked internal method
```

## Best Practices

### 1. Patch Where Objects Are Used, Not Where They're Defined
```python
# ❌ Wrong - patching at source
with patch('requests.get'):  # This might not work

# ✅ Correct - patching where imported
with patch('mymodule.requests.get'):  # This works
```

### 2. Use Specific Return Values
```python
# ❌ Vague
mock.return_value = MagicMock()

# ✅ Specific
mock.return_value = {'user_id': 123, 'name': 'Test User'}
```

### 3. Configure Mock Behavior Realistically
```python
# For Event objects, implement realistic state
event_state = {'is_set': False}

def mock_set():
    event_state['is_set'] = True

def mock_is_set():
    return event_state['is_set']

mock_event.set.side_effect = mock_set
mock_event.is_set.side_effect = mock_is_set
```

### 4. Use Fixtures for Reusable Mocks
```python
@pytest.fixture
def mock_database():
    with patch('myapp.database.connect') as mock_conn:
        mock_conn.return_value.execute.return_value = [{'id': 1}]
        yield mock_conn
```

```python
@pytest.fixture
def mock_database():
    with patch('myapp.database.connect') as mock_conn:
        mock_conn.return_value.execute.return_value = [{'id': 1}]
        yield mock_conn

# Usage in multiple tests
def test_user_query(mock_database):
    result = get_user(123)
    mock_database.return_value.execute.assert_called_once()

def test_user_update(mock_database):
    update_user(123, {'name': 'New Name'})
    mock_database.return_value.execute.assert_called()
```

### 5. Combine Mocks with Fixtures
```python
@pytest.fixture
def configured_service(mock_database, mock_api, sample_config):
    """Fixture that combines multiple mocks into a ready-to-test service."""
    return MyService(config=sample_config)

def test_service_operation(configured_service):
    # Service already has all mocks configured
    result = configured_service.process_data()
    assert result.success
```

### 6. Verify Mock Calls
```python
# Verify method was called
mock.assert_called_once()

# Verify method was called with specific arguments
mock.assert_called_with('expected', 'arguments')

# Verify method was not called
mock.assert_not_called()
```

## Troubleshooting

### Problem: Mock Not Working
**Cause**: Patching wrong import path
**Solution**: Patch where the object is imported, not where it's defined

```python
# If your module does: from requests import get
# Then patch: 'mymodule.get'
# Not: 'requests.get'
```

### Problem: AttributeError on Mock
**Cause**: Accessing attributes that aren't configured
**Solution**: Configure mock attributes explicitly

```python
mock_response = MagicMock()
mock_response.status_code = 200
mock_response.json.return_value = {'data': 'value'}
```

### Problem: Mock Called Multiple Times
**Cause**: Test isolation issues
**Solution**: Reset mocks between tests or use fresh fixtures

```python
@pytest.fixture
def fresh_mock():
    with patch('module.function') as mock:
        yield mock
    # Mock is automatically reset after each test
```

### Problem: Complex Object Mocking
**Cause**: Not understanding object construction
**Solution**: Mock the class, not the instance

```python
# ❌ Trying to mock an instance
with patch('threading.Event()'):  # This doesn't work

# ✅ Mock the class constructor
with patch('threading.Event') as MockEvent:
    mock_instance = MagicMock()
    MockEvent.return_value = mock_instance
```

## Example Test Structure

```python
def test_my_function(mock_external_api, mock_file_system):
    # Arrange - set up mock behaviors
    mock_external_api.return_value = {'status': 'success'}
    mock_file_system.read_data = 'config content'
    
    # Act - call the function being tested
    result = my_function()
    
    # Assert - verify results and mock calls
    assert result == expected_result
    mock_external_api.assert_called_once()
    mock_file_system.assert_called_with('config.txt')
```

---

**Remember**: The goal of mocking is to isolate the code under test by removing dependencies. Mock just enough to make your test predictable and fast, but not so much that you're testing the mocks instead of your code!
