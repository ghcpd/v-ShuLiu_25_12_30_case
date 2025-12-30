# Run baseline, optimized benchmark and pytest, printing results to the terminal
python -V
python input_new/baseline_task_queue_v2.py
Write-Host "\n-- Optimized benchmark --"
python task_queue_optimized.py --benchmark
Write-Host "\n-- Running pytest --"
pytest -q -rA
