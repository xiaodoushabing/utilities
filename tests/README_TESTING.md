# 🧪 Testing Hub for LogManager

Your one-stop resource for testing workflows, troubleshooting, and practical tips.

## 🚀 Quick Start

1. **Install testing tools:**
   ```bash
   pip install -r requirements-test.txt
   ```

2. **Run your first tests:**
   ```bash
   pytest test_logmanager_basics.py -v
   ```

3. **See test coverage:**
   ```bash
   pytest --cov=logmanager --cov-report=html
   ```

## 📁 Test Files Overview

| File | Purpose | When to Use |
|------|---------|-------------|
| `test_logmanager_basics.py` | 🟢 Learn pytest fundamentals | First time learning pytest |
| `test_logmanager_comprehensive.py` | 🟡 Production-ready test suite | Understanding real test patterns |
| `test_logger.py` | 🟢 Original usage examples | Comparing manual vs automated testing |

## 📚 Learning Path

| Step | Resource | Focus |
|------|----------|-------|
| **1** | `PYTEST_LEARNING_GUIDE.md` | **Learn concepts step-by-step** |
| **2** | This file (`README_TESTING.md`) | **Apply knowledge practically** |
| **3** | `test_logmanager_comprehensive.py` | **Study real-world examples** |

---

## 🔧 Testing Workflows

### Daily Development Workflow

```bash
# 1. Run tests while coding (fast feedback)
pytest -x  # Stop on first failure

# 2. Run specific test for feature you're working on
pytest -k "test_add_handler" -v

# 3. Run full suite before committing
pytest --cov=logmanager --cov-report=term-missing

# 4. Check what you missed
pytest --cov=logmanager --cov-report=html
# Open htmlcov/index.html in browser
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
pytest -k "handler"  # All tests with "handler" in name
pytest -k "not integration"  # Skip integration tests
pytest -k "add and not remove"  # Complex filtering

# Run tests by markers (if you add them)
pytest -m "slow"     # Run only slow tests
pytest -m "not slow" # Skip slow tests
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
```python
class TestLogManagerInitialization:
    """Group tests for LogManager.__init__()"""
    pass

class TestHandlerManagement:
    """Group tests for add/update/remove handler methods"""
    pass

class TestLoggerOperations:
    """Group tests for get_logger and logger configuration"""
    pass
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
@patch('logmanager.logger')
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
        with patch('logmanager.logger'):
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

---

## 📊 Coverage Tips

### Understanding Coverage Reports
```bash
# Generate detailed coverage report
pytest --cov=logmanager --cov-report=html --cov-report=term-missing

# Focus on missing lines
pytest --cov=logmanager --cov-report=term-missing | grep MISS
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

### Testing with Real Files (When Necessary)
```python
def test_actual_file_creation_integration(tmp_path):
    """Sometimes you need to test the real thing."""
    log_file = tmp_path / "app.log"
    
    # Test with real LogManager (no mocking)
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
        # Cleanup - remove all handlers to close files
        from loguru import logger
        logger.remove()
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

### Speed Up Your Testing
- Use `pytest-xdist` for parallel execution: `pip install pytest-xdist`
- Run with: `pytest -n auto` (uses all CPU cores)
- Use `pytest-watch` for automatic re-running: `pip install pytest-watch`

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
    - run: pip install -r requirements-test.txt
    - run: pytest --cov=logmanager --cov-report=xml
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

**Need help?** Check the `PYTEST_LEARNING_GUIDE.md` for step-by-step concepts, or dive into `test_logmanager_comprehensive.py` for real examples.
