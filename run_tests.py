#!/usr/bin/env python3
"""Windows PowerShell test runner script"""

import subprocess
import sys

def main():
    """Run all tests"""
    print("="*70)
    print("RUNNING ALL TESTS")
    print("="*70)
    
    # Run unit tests
    print("\n[1/2] Running unit tests...")
    result = subprocess.run([sys.executable, '-m', 'pytest', 'tests/test_unit.py', '-v'], cwd='.')
    if result.returncode != 0:
        print("Unit tests FAILED")
        return result.returncode
    
    print("\n[2/2] Running integration tests...")
    result = subprocess.run([sys.executable, '-m', 'pytest', 'tests/test_integration.py', '-v'], cwd='.')
    if result.returncode != 0:
        print("Integration tests FAILED")
        return result.returncode
    
    print("\n" + "="*70)
    print("ALL TESTS PASSED ✓")
    print("="*70)
    return 0

if __name__ == '__main__':
    sys.exit(main())
