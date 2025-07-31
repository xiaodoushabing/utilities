# 🧪 Testing Hub for LogManager

Your one-stop resource for testing workflows, troubleshooting, and practical tips.

## 🚀 Quick Start

1. **Create and activate virtual environment:**
   ```bash
   # Create virtual environment
   python -m venv .venv
   
   # Activate environment (Windows)
   .venv\Scripts\activate
   
   # Activate environment (Linux/Mac)
   source .venv/bin/activate
   ```

2. **Install project in development mode:**
   ```bash
   # Install project with test dependencies
   pip install -e ".[test]"
   ```

3. **Run your first tests:**
   ```bash
   pytest tests/test_logmanager.py -v
   ```

4. **See test coverage:**
   ```bash
   pytest --cov=src --cov-report=html
   ```

## 📁 Test Files Overview

| File | Purpose | When to Use |
|------|---------|-------------|
| `test_logmanager.py` | 🟢 **Unit Tests** - Fast, isolated component testing | Daily development and debugging |
| `test_logmanager_integration.py` | 🟡 **Integration Tests** - Complex workflow testing | Before commits and releases |
| `test_logger.py` | 🟢 Original usage examples | Comparing manual vs automated testing |
| `conftest.py` | **Shared fixtures** and test configuration | Understanding pytest setup |

## 🏷️ Test Categories & Markers

We use pytest markers to organize tests by type and speed:

| Marker | Purpose | Files | Run Command |
|--------|---------|-------|-------------|
| `@pytest.mark.unit` | Fast, isolated tests | `test_logmanager.py` | `pytest -m unit` |
| `@pytest.mark.integration` | Complex workflow tests | `test_logmanager_integration.py` | `pytest -m integration` |

### Quick Commands for Different Test Types

```bash
# Run only fast unit tests (daily development)
pytest -m unit -v

# Run only integration tests (before commits)
pytest -m integration -v

# Run all tests
pytest tests/ -v

# Skip slow integration tests
pytest -m "not integration" -v
```

## 📚 Learning Path

| Step | Resource | Focus |
|------|----------|-------|
| **1** | `PYTEST_LEARNING_GUIDE.md` | **Learn concepts step-by-step** |
| **2** | This file (`README_TESTING.md`) | **Apply knowledge practically** |
| **3** | `test_logmanager.py` | **Study unit test examples** |
| **4** | `test_logmanager_integration.py` | **Study integration test patterns** |

---

## 🔧 Testing Workflows

### Daily Development Workflow

```bash
# 1. Run fast unit tests while coding (instant feedback)
pytest -m unit -x  # Stop on first failure

# 2. Run specific test for feature you're working on
pytest -k "test_add_handler" -v

# 3. Run integration tests before committing
pytest -m integration -v

# 4. Run full suite with coverage before pushing
pytest --cov=src --cov-report=term-missing
```

### Test Discovery by Category

```bash
# See what unit tests are available
pytest -m unit --collect-only

# See what integration tests are available  
pytest -m integration --collect-only

# Run tests by file
pytest tests/test_logmanager.py -v              # Unit tests only
pytest tests/test_logmanager_integration.py -v  # Integration tests only
```

### Debugging Workflow

```bash
# When tests fail, get more info
pytest -v -s --tb=long  # Verbose output with full tracebacks

# Drop into debugger at failure point
pytest --pdb

# Run only tests that failed last time
pytest --lf

# Run failed tests first, then continue with rest
pytest --ff
```

### Test Discovery and Filtering

```bash
# See all tests without running them
pytest --collect-only

# Run tests matching a pattern
pytest -k "handler" -v  # All tests with "handler" in name
pytest -k "filter" -v   # All handler filter tests  
pytest -k "not integration" -v  # Skip integration tests

# Run tests by markers
pytest -m unit -v           # Run only unit tests (fast)
pytest -m integration -v    # Run only integration tests (slower)
pytest -m "not integration" -v  # Skip slow integration tests

# Combine markers and patterns
pytest -m unit -k "handler" -v  # Unit tests about handlers only
```

---

## 🎯 Test Organization Best Practices

### File Naming Conventions
- `test_*.py` - Test files (pytest discovers these automatically)
- `conftest.py` - Shared fixtures and configuration
- `*_test.py` - Alternative naming (also works)

### Test Function Naming
```python
# ✅ Good - descriptive and specific
def test_add_handler_stores_config_in_internal_map():
def test_duplicate_handler_names_raise_assertion_error():
def test_invalid_config_missing_sink_raises_key_error():

# ❌ Avoid - too generic
def test_handler():
def test_error():
def test_config():
```

### Test Class Organization

Our current test structure follows these patterns:

**Unit Tests (`test_logmanager.py`)**:
```python
class TestLogManagerBasics:
    """Basic functionality and creation tests"""
    
class TestLogManagerInitialization:  
    """Initialization behavior and environment setup"""
    
class TestMappingCleanup:
    """Bidirectional handler-logger relationship management"""
    
class TestHandlerFilterBehavior:
    """Handler filter function creation and level testing"""
    
class TestHandlerManagement:
    """Handler CRUD operations (add/update/remove)"""
    
class TestLoggerManagement:
    """Logger CRUD operations and retrieval"""
    
class TestNonexistentEntityOperations:
    """Error handling for operations on missing entities"""
```

**Integration Tests (`test_logmanager_integration.py`)**:
```python
class TestIntegrationScenarios:
    """Complex workflows combining multiple operations"""
    # - Complete workflow simulation
    # - Bidirectional mapping consistency
    # - Edge cases and error handling in realistic scenarios
```

---

## 🐛 Common Testing Pitfalls & Solutions

### Problem: Tests are Slow
```python
# ❌ Creating real files in every test
def test_file_logging():
    lm = LogManager()
    lm.add_handler('file', {'sink': 'real_file.log'})  # Slow!

# ✅ Mock the logger to avoid file I/O
@patch('main.logger')  # Updated import path
def test_file_logging(mock_logger):
    lm = LogManager()
    lm.add_handler('file', {'sink': 'real_file.log'})  # Fast!
```

### Problem: Tests Depend on Each Other
```python
# ❌ Tests sharing state
class TestBad:
    lm = LogManager()  # Shared instance - BAD!
    
    def test_add_handler(self):
        self.lm.add_handler('test', config)
        
    def test_handler_exists(self):
        # This depends on previous test running first!
        assert 'test' in self.lm._handlers_map

# ✅ Fresh instance per test
class TestGood:
    @pytest.fixture(autouse=True)
    def setup(self):
        with patch('main.logger'):  # Updated import path
            self.lm = LogManager()  # Fresh per test
```

### Problem: Hard to Debug Test Failures
```python
# ❌ Generic assertions
assert result  # What exactly failed?
assert len(handlers) == 2  # Which handlers? What was the actual count?

# ✅ Descriptive assertions with helpful messages
assert result is not None, f"Expected LogManager instance, got {result}"
assert len(handlers) == 2, f"Expected 2 handlers, got {len(handlers)}: {list(handlers.keys())}"

# ✅ Better - test specific behavior
assert 'console_handler' in handlers
assert 'file_handler' in handlers
```

### Problem: Inconsistent Test Environment
```python
# ❌ Tests affected by environment
def test_config_loading():
    # Might fail if file exists from previous run
    lm = LogManager('config.yaml')

# ✅ Use pytest's tmp_path fixture
def test_config_loading(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(test_config))
    lm = LogManager(str(config_file))  # Isolated!
```

### Problem: Import Errors
```python
# ❌ Module not found errors
ModuleNotFoundError: No module named 'main'
ModuleNotFoundError: No module named 'logmanager'

# ✅ Solutions:
# 1. Install in development mode
pip install -e ".[test]"

# 2. Verify your virtual environment is activated
which python  # Should point to .venv/bin/python

# 3. Check project structure - LogManager should be in src/main/__init__.py
# 4. Update imports in test files to use 'from main import LogManager'
```

---

## 📊 Coverage Tips

### Understanding Coverage Reports
```bash
# Generate detailed coverage report
pytest --cov=src --cov-report=html --cov-report=term-missing

# Focus on missing lines
pytest --cov=src --cov-report=term-missing | grep MISS
```

### Coverage Goals
- **80%+ overall** - Good starting point
- **95%+ for critical paths** - LogManager core functionality
- **Don't chase 100%** - Focus on meaningful tests, not coverage numbers

### What NOT to Test
- Third-party library code (loguru internals)
- Python built-ins
- Simple property getters/setters
- Configuration constants

---

## 🔍 Advanced Testing Techniques

### Parametrized Testing (Real Examples from Our Suite)

```python
# From TestHandlerFilterBehavior - testing multiple scenarios efficiently
@pytest.mark.parametrize("logger_name,record_level,expected_result,test_description", [
    ("test_logger", 50, True, "allows correct logger with CRITICAL level (50 >= 40)"),
    ("test_logger", 40, True, "allows correct logger with ERROR level (40 >= 40)"),
    ("test_logger", 30, False, "blocks correct logger with WARNING level (30 < 40)"),
    ("different_logger", 50, False, "blocks logs from unassociated loggers"),
])
def test_handler_filter_behavior(self, handler_logger_setup, mock_logger, 
                               logger_name, record_level, expected_result, test_description):
    # Single test method handles 8+ different scenarios
```

### Fixture-Based Setup (Eliminates Duplication)

```python
# From TestHandlerFilterBehavior - shared setup for all filter tests
@pytest.fixture
def handler_logger_setup(self, log_manager):
    """Setup handler and logger for filter testing."""
    # Add a handler with ERROR level threshold
    log_manager.add_handler("test_handler", {
        'sink': 'sys.stdout', 'format': 'simple', 'level': 'ERROR'
    })
    
    # Add a logger that uses this handler with DEBUG level
    log_manager.add_logger("test_logger", [
        {'handler': 'test_handler', 'level': 'DEBUG'}
    ])
    
    # Return the filter function for testing
    return log_manager._make_handler_filter("test_handler")
```

### Testing Bidirectional Relationships

```python
# From TestMappingCleanup - ensuring consistency in complex data structures
def test_bidirectional_mapping_creation(self, log_manager):
    """Test that adding handler and logger creates proper bidirectional mapping."""
    # Setup: Add handler and logger
    log_manager.add_handler("test_handler", {...})
    log_manager.add_logger("test_logger", [{'handler': 'test_handler', 'level': 'DEBUG'}])
    
    # Verify bidirectional mapping exists
    # Side 1: _loggers_map should reference handler
    assert "test_handler" in [h["handler"] for h in log_manager._loggers_map["test_logger"]]
    # Side 2: _handlers_map should reference logger  
    assert "test_logger" in log_manager._handlers_map["test_handler"]["loggers"]
```

### Testing with Real Files (When Necessary)

```python
# From integration tests - testing actual file operations when needed
def test_actual_file_creation_integration(tmp_path):
    """Sometimes you need to test the real thing."""
    log_file = tmp_path / "app.log"
    
    # Test with real LogManager (no mocking for integration)
    lm = LogManager()
    
    try:
        lm.add_handler('file', {
            'sink': str(log_file),
            'level': 'INFO', 
            'format': '{message}'
        })
        
        logger = lm.get_logger('test')
        logger.info("Test message")
        
        # Verify file was created and contains message
        assert log_file.exists()
        content = log_file.read_text()
        assert "Test message" in content
        
    finally:
        # Cleanup - LogManager handles this automatically
        lm._cleanup()
```

### Unit vs Integration Test Strategy

**Unit Tests** - Mock external dependencies:
```python
# Mock the logger to isolate LogManager logic
def test_add_handler(self, log_manager, mock_logger):
    mock_logger.reset_mock()
    log_manager.add_handler("test_handler", config)
    
    # Verify internal state without touching real loguru
    mock_logger.add.assert_called_once()
    assert "test_handler" in log_manager._handlers_map
```

**Integration Tests** - Test real workflows:  
```python
# Test complete workflows with multiple operations
def test_complete_workflow_simulation(self, log_manager, mock_logger):
    # 1. Get pre-configured loggers
    logger_a = log_manager.get_logger("logger_a") 
    logger_b = log_manager.get_logger("logger_b")
    
    # 2. Add new handlers and loggers
    log_manager.add_handler("handler_console_fire", {...})
    log_manager.add_logger("logger_c", [...])
    
    # 3. Update configurations
    log_manager.update_handler("handler_console_fire", {...})
    log_manager.update_logger("logger_c", [...])
    
    # 4. Test cleanup behavior
    log_manager.remove_logger("logger_c")
    log_manager.remove_handler("handler_console")
```

### Performance Testing
```python
import time

def test_handler_creation_performance():
    """Ensure handler creation is fast enough."""
    start_time = time.time()
    
    with patch('logmanager.logger'):
        lm = LogManager()
        for i in range(100):
            lm.add_handler(f'handler_{i}', {
                'sink': 'sys.stdout',
                'level': 'INFO',
                'format': 'simple'
            })
    
    duration = time.time() - start_time
    assert duration < 1.0, f"Creating 100 handlers took {duration:.2f}s (too slow!)"
```

### Error Message Testing
```python
def test_meaningful_error_messages():
    """Ensure error messages help users understand what went wrong."""
    lm = LogManager()
    
    with pytest.raises(KeyError) as exc_info:
        lm.add_handler('test', {})  # Missing required keys
    
    error_msg = str(exc_info.value)
    assert 'sink' in error_msg, "Error should mention missing 'sink'"
    assert 'level' in error_msg, "Error should mention missing 'level'"
```

---

## 🎖️ Testing Maturity Levels

### Level 1: Basic Testing 🟢
- [ ] Can run existing tests
- [ ] Understand test output
- [ ] Write simple assert statements
- [ ] Use basic fixtures

### Level 2: Confident Testing 🟡
- [ ] Write tests for new features
- [ ] Use mocking effectively
- [ ] Understand parametrized tests
- [ ] Debug failing tests

### Level 3: Testing Expert 🔴
- [ ] Practice test-driven development
- [ ] Design testable code
- [ ] Mentor others on testing
- [ ] Contribute to testing infrastructure

---

## 🚀 Pro Tips

### Environment Management
```bash
# Create project-specific environment
python -m venv .venv

# Always activate before working
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install in development mode (editable)
pip install -e ".[test]"

# Verify installation
python -c "from main import LogManager; print('✅ LogManager imported successfully')"

# Deactivate when done
deactivate
```

### Speed Up Your Testing
- Use `pytest-xdist` for parallel execution: `pip install pytest-xdist`
- Run with: `pytest -n auto` (uses all CPU cores)
- Use `pytest-watch` for automatic re-running: `pip install pytest-watch`
- **Separate unit and integration tests**: Run fast unit tests during development with `pytest -m unit`

### Test Markers Configuration

Our `pyproject.toml` includes custom markers:
```toml
[tool.pytest.ini_options]
markers = [
    "unit: marks tests as unit tests (fast, isolated tests for individual components)",
    "integration: marks tests as integration tests (slower tests that verify multiple components work together)",
]
```

This eliminates pytest warnings and provides clear test categorization.

### IDE Integration
- **VS Code**: Install Python Test Explorer extension
- **PyCharm**: Built-in pytest support
- **Vim/Neovim**: Use vim-test plugin

### CI/CD Integration
```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - uses: actions/setup-python@v2
      with:
        python-version: '3.8'
    - name: Install dependencies
      run: pip install -e ".[test]"
    - name: Run tests
      run: pytest --cov=src --cov-report=xml
    - uses: codecov/codecov-action@v1
```

---

## 🎉 Final Words

Testing is a **skill that compounds**. Every test you write:
- Makes future changes safer
- Documents expected behavior  
- Builds confidence in your code
- Teaches you about your own design

**Start small, be consistent, and keep learning!** 🧪✨

---

**Need help?** Check the `PYTEST_LEARNING_GUIDE.md` for step-by-step concepts, or dive into our test files:
- `test_logmanager.py` for unit test examples  
- `test_logmanager_integration.py` for integration test patterns
