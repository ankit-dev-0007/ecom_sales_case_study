#!/bin/bash
# Test runner script for Unix/Linux/Mac

echo "Running E-commerce Data Processing Tests..."
echo "============================================"

# Run tests with coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "✓ All tests passed!"
else
    echo ""
    echo "✗ Some tests failed!"
    exit 1
fi
