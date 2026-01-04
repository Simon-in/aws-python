# Contributing to AWS Python Project

Thank you for your interest in contributing to our project! We welcome contributions from everyone, regardless of experience level.

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Git
- AWS CLI (for testing AWS-related functionality)
- A GitHub account

### Development Setup

1. **Fork the repository**
   - Click the "Fork" button at the top right of the repository page
   - Clone your forked repository locally

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -e .[dev]
   ```

4. **Set up pre-commit hooks (optional but recommended)**
   ```bash
   pre-commit install
   ```

## Development Process

### 1. Create a Branch

Create a new branch for your feature or bug fix:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b bugfix/your-bugfix-name
```

### 2. Write Code

- Follow the project's code style (black for formatting, isort for imports)
- Write docstrings for all functions and classes
- Add type hints where appropriate
- Keep code concise and well-organized

### 3. Write Tests

- Add unit tests for new functionality
- Ensure all existing tests pass
- Aim for high test coverage

### 4. Run Tests

```bash
pytest
# Run tests with coverage
pytest --cov=modules
```

### 5. Check Code Style

```bash
# Format code with black
black .

# Sort imports with isort
isort .

# Check code quality with flake8
flake8 .
```

### 6. Commit Changes

Use descriptive commit messages:

```bash
git add .
git commit -m "Add feature: Description of your feature"
# or
git commit -m "Fix bug: Description of the bug and fix"
```

### 7. Push Changes

```bash
git push origin your-branch-name
```

### 8. Create a Pull Request

- Go to your forked repository on GitHub
- Click the "Compare & pull request" button
- Fill out the pull request template
- Click "Create pull request"

## Code Style Guidelines

- **Formatting**: Use Black for code formatting
- **Imports**: Use isort for import sorting
- **Docstrings**: Use Google-style docstrings
- **Type Hints**: Use Python type hints for function parameters and return values
- **Naming Conventions**:
  - Classes: CamelCase
  - Functions: snake_case
  - Variables: snake_case
  - Constants: UPPER_SNAKE_CASE

## Reporting Issues

When reporting issues, please include:

- A clear and descriptive title
- A detailed description of the issue
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Your environment (Python version, OS, etc.)
- Any relevant logs or error messages

## Feature Requests

When requesting new features, please include:

- A clear and descriptive title
- A detailed description of the requested feature
- Use cases for the feature
- Any relevant examples or mockups

## Code of Conduct

Please refer to our [Code of Conduct](CODE_OF_CONDUCT.md) for guidelines on community behavior.

## License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to our project! Your help is greatly appreciated.
