import importlib
import tempfile
from pathlib import Path
import json

mods=['tests.test_idempotency','tests.test_integration_basic','tests.test_metrics_and_concurrency','tests.test_retry_and_deadletter']
report = {'tests': [], 'summary': {'total':0,'passed':0,'failed':0}}
for m in mods:
    mod = importlib.import_module(m)
    tests = [a for a in dir(mod) if a.startswith('test_')]
    for t in tests:
        func = getattr(mod, t)
        kwargs = {}
        if 'tmp_path' in func.__code__.co_varnames:
            kwargs['tmp_path'] = Path(tempfile.mkdtemp())
        try:
            func(**kwargs)
            status = 'PASS'
            report['summary']['passed'] += 1
        except AssertionError as e:
            status = 'FAIL'
            report['summary']['failed'] += 1
        except Exception as e:
            status = 'ERROR'
            report['summary']['failed'] += 1
        report['tests'].append({'module': m, 'name': t, 'status': status})
        report['summary']['total'] += 1

with open('test_report.json', 'w', encoding='utf-8') as f:
    json.dump(report, f, indent=2)

with open('test_report.txt', 'w', encoding='utf-8') as f:
    s = report['summary']
    f.write(f"Tests: {s['total']}  Passed: {s['passed']}  Failed: {s['failed']}\n\n")
    for t in report['tests']:
        f.write(f"{t['status']} {t['module']}.{t['name']}\n")
    f.write('\nArtifacts:\n')
    f.write('  benchmark_result.json\n')
    f.write('  benchmark_comparison.json\n')

print('Wrote test_report.json and test_report.txt')