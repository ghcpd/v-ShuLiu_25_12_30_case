import importlib
from pathlib import Path
import tempfile
mods=['tests.test_idempotency','tests.test_integration_basic','tests.test_metrics_and_concurrency','tests.test_retry_and_deadletter']
for m in mods:
    print('===', m)
    mod = importlib.import_module(m)
    tests = [a for a in dir(mod) if a.startswith('test_')]
    print('found', tests)
    for t in tests:
        func = getattr(mod, t)
        print('running', t, 'args', func.__code__.co_argcount)
        kwargs = {}
        if 'tmp_path' in func.__code__.co_varnames:
            kwargs['tmp_path'] = Path(tempfile.mkdtemp())
        try:
            func(**kwargs)
            print('OK', t)
        except Exception as e:
            print('ERR', t, e)
            import traceback; traceback.print_exc()
print('done')
