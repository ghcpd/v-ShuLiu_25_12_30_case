import importlib
mods=['tests.test_idempotency','tests.test_integration_basic','tests.test_metrics_and_concurrency','tests.test_retry_and_deadletter']
for m in mods:
    try:
        mod = importlib.import_module(m)
        print(m, [a for a in dir(mod) if a.startswith('test_')])
    except Exception as e:
        print('ERROR importing', m, e)
