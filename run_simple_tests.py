"""Lightweight test runner that imports test modules and runs functions named test_*.
This is a fallback when pytest is not available or environment has plugin conflicts.
"""
import importlib
import os
import sys
import traceback

TEST_DIR = 'tests'


def find_test_modules():
    for name in os.listdir(TEST_DIR):
        if name.startswith('test_') and name.endswith('.py'):
            yield f"{TEST_DIR}.{name[:-3]}"


def run():
    total = 0
    passed = 0
    failed = 0
    failures = []
    mods = list(find_test_modules())
    print('Discovered test modules:', mods)
    for modname in mods:
        print('Processing module', modname)
        try:
            mod = importlib.import_module(modname)
        except Exception as e:
            print(f'ERROR importing {modname}: {e}')
            traceback.print_exc()
            failed += 1
            continue
        import inspect
    import tempfile
    from pathlib import Path
    for attr in dir(mod):
            if attr.startswith('test_') and callable(getattr(mod, attr)):
                total += 1
                func = getattr(mod, attr)
                # prepare kwargs for common pytest fixtures we use (tmp_path)
                sig = inspect.signature(func)
                kwargs = {}
                if 'tmp_path' in sig.parameters:
                    tempd = tempfile.mkdtemp()
                    kwargs['tmp_path'] = Path(tempd)
                try:
                    print(f'RUNNING {modname}.{attr}')
                    func(**kwargs)
                    print(f'PASS {modname}.{attr}')
                    passed += 1
                except AssertionError as e:
                    print(f'FAIL {modname}.{attr}: {e}')
                    traceback.print_exc()
                    failed += 1
                except Exception as e:
                    print(f'ERROR {modname}.{attr}: {e}')
                    traceback.print_exc()
                    failed += 1
    print('\nSummary:')
    print(f'  Total: {total}  Passed: {passed}  Failed: {failed}')
    return 0 if failed == 0 else 2


if __name__ == '__main__':
    sys.exit(run())
