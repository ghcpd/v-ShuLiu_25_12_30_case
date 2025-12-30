#!/bin/bash
# Test runner for Linux/macOS/Git Bash

echo "======================================================================="
echo "RUNNING ALL TESTS"
echo "======================================================================="

# Unit tests
echo ""
echo "[1/2] Running unit tests..."
python -m pytest tests/test_unit.py -v --tb=short
if [ $? -ne 0 ]; then
    echo "Unit tests FAILED"
    exit 1
fi

# Integration tests
echo ""
echo "[2/2] Running integration tests..."
python -m pytest tests/test_integration.py -v --tb=short
if [ $? -ne 0 ]; then
    echo "Integration tests FAILED"
    exit 1
fi

echo ""
echo "======================================================================="
echo "ALL TESTS PASSED ✓"
echo "======================================================================="
