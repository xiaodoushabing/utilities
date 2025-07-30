# Pytest Unit Testing - Step-by-Step Learning Guide for LogManager

## 🎯 What You'll Learn

By the end of this guide, you'll master:
- **Writing effective unit tests** with pytest
- **Testing your LogManager class** systematically  
- **Using fixtures, mocking, and parametrization**
- **Best practices** for maintainable tests

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
# Required imports
import pytest
import yaml
from unittest.mock import patch, MagicMock
from logmanager import LogManager

def test_logmanager_creates_successfully():
    """Test that LogManager can be created with default settings."""
    # Arrange - (no setup needed)
    
    # Act - Create the LogManager
    lm = LogManager()
    
    # Assert - Verify it was created correctly
    assert lm is not None
    assert lm._config_path == LogManager.DEFAULT_CONFIG_PATH
```

**Key Points:**
- Test names should describe what you're testing
- Use simple `assert` statements
- Add docstrings to explain the test purpose

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
    """Provide test configuration data."""
    return {
        'formats': {'simple': '{message}'},
        'handlers': {'console': {'sink': 'sys.stdout', 'format': 'simple', 'level': 'INFO'}},
        'loggers': {'test_logger': [{'handler': 'console', 'level': 'INFO'}]}
    }

def test_config_loading(sample_config, tmp_path):
    """Test that LogManager loads configuration correctly."""
    # Arrange - Create temporary config file
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(yaml.dump(sample_config))
    
    # Act - Load LogManager with config
    with patch('logmanager.logger'):  # Mock to avoid side effects
        lm = LogManager(config_path=str(config_file))
    
    # Assert - Verify config was loaded
    assert lm.config == sample_config
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
# Without mocking - BAD for tests:
lm = LogManager()  
# ↳ Creates real log files
# ↳ Prints to console
# ↳ Slow and messy

# With mocking - GOOD for tests:
with patch('logmanager.logger') as mock_logger:
    lm = LogManager()
    # ↳ Logger operations are "faked"
    # ↳ No files created, fast and clean
```

#### Basic Mocking Example

```python
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

**Rule:** Patch where the code **imports** the object, not where it's **defined**.

```python
# In logmanager.py:
from loguru import logger  # Creates logmanager.logger

# So patch:
patch('logmanager.logger')  # ✅ Correct
# NOT:
patch('loguru.logger')      # ❌ Wrong - your code won't see this
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

This creates 3 separate tests:
- `test_level_normalization[debug-DEBUG]`
- `test_level_normalization[INFO-INFO]`
- `test_level_normalization[Warning-WARNING]`

#### Testing Error Cases

```python
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

#### Testing Handler Management

```python
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
with pytest.raises(AssertionError, match="already exists"):
    lm.add_handler('duplicate_name', config)
```

This means:
- "I expect an `AssertionError` to be raised"
- "The error message should contain 'already exists'"
- "If no error occurs, the test fails"
- "If wrong error type occurs, the test fails"

---

### 🔗 Step 6: Integration Testing

Test complete workflows that match real usage:

```python
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
pytest test_logmanager_comprehensive.py -v

# Run specific test
pytest test_logmanager_comprehensive.py::test_name -v

# Run with coverage report
pytest --cov=logmanager --cov-report=html

# Stop on first failure
pytest -x

# Show local variables on failure
pytest -l
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
1. Open `test_logmanager_comprehensive.py`
2. Find an existing test and understand its structure
3. Write a simple test for LogManager initialization

### Exercise 2: Create a Fixture
1. Create a fixture that provides a configured LogManager
2. Use it in multiple tests
3. Add cleanup if needed

### Exercise 3: Use Parametrized Testing
1. Test log level normalization with different inputs
2. Test invalid configurations with various missing keys
3. Observe how pytest reports each parameter combination

### Exercise 4: Test Error Conditions
1. Test what happens with duplicate handler names
2. Test missing configuration keys
3. Test invalid file paths

---

## 📋 Testing Checklist

For each LogManager method, test:
- [ ] **Happy path** - Works with valid inputs
- [ ] **Edge cases** - Boundary conditions  
- [ ] **Error cases** - Invalid inputs raise appropriate errors
- [ ] **State changes** - Internal state updates correctly
- [ ] **Side effects** - External calls happen as expected

---

## 💡 Key Takeaways

- **Start simple** - Begin with basic functionality
- **Use AAA pattern** - Arrange, Act, Assert
- **Mock external dependencies** - Keep tests fast and isolated
- **Test error cases** - Don't just test happy paths
- **Use descriptive names** - Tests should tell a story
- **Keep tests independent** - Each test should run alone

**Remember:** Good tests are an investment. They catch bugs early, enable confident refactoring, and document how your code should behave.

---

## 🚀 Next Steps

Once you've mastered these basics:
1. Practice test-driven development (TDD)
2. Learn property-based testing with Hypothesis
3. Explore advanced mocking techniques
4. Study mutation testing to test your tests
