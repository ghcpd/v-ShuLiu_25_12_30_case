```markdown
# Baseline Notes (Scenario 2)

This baseline intentionally contains performance and concurrency flaws:
- Global lock around a shared list used as a queue
- No idempotency or retry policy
- Blocking I/O without jitter or backpressure
- No metrics or logging to identify bottlenecks

Use this as the starting point to measure and improve throughput and P95 latency under 100 concurrent producers.

说明：此基线有意包含性能与并发缺陷：
共享列表作为队列并加全局锁
无幂等性或重试策略
阻塞式 I/O，缺少抖动（jitter）与背压（backpressure）
无指标或日志来识别瓶颈
建议：将其作为起点，在 100 个并发生产者条件下，测量并提升吞吐量与 P95 延迟。
```
