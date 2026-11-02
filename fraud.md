# Fraud producer, consumer, topic/s and event/s

## Topic configs

Topic: `digitalpayments.fraud.score`. The fraud producer publishes the fraud score (low, medium or high) for each payment here.

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digitalpayments.fraud.score \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220752000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=65536
```

| Setting | Value | Reason |
|---|---|---|
| `--topic` | `digitalpayments.fraud.score` | Same `<domain>.<entity>.<event>` convention as the payment topic. It names the fraud score events this topic carries. |
| `--partitions` | `100` | Matches the payment topic. Kafka's default partitioner maps a key to the same partition index whenever both topics have the same count. So a `payment_id` lands on partition N in both topics, and a downstream process can join score and payment on one partition. The count also covers the worst case of 1000 Mb/s with 10 Mb/s per consumer. |
| `--replication-factor` | `3` | The fraud score is a regulated fraud event. The cluster has 3 brokers, so 3 replicas survive one broker failure with no loss. |
| `retention.ms` | `220752000000` | 7 years, as the brief requires for fraud events: 2555 days × 86,400,000 ms. The same storage caveat applies as for the payment topic. |
| `cleanup.policy` | `delete` | Each fraud score is a recorded decision for a payment. It is an immutable event, not a current state. Compaction would erase the history that a dispute or audit needs. |
| `min.insync.replicas` | `2` | With producers on `acks=all`, a score is acknowledged only once 2 replicas hold it. One broker can fail without losing a score or stopping fraud decisions. |
| `compression.type` | `lz4` | The fraud path has the tightest budget, lag under 50 ms. lz4 has the lowest compression and decompression CPU cost, so it adds the least delay. Fraud score events are small, so the storage saving from zstd would be small too. |
| `max.message.bytes` | `65536` (64 KiB) | A fraud score event holds a score, a band, a model version, and a few reason codes, well under 1 KB. The 64 KiB limit guards against a runaway reason list and keeps the topic's messages small for the 50 ms path. |

## Consumer configs

Consumer group: `digitalpayments-fraud-scoring`. This is the real-time fraud scoring consumer. It reads each payment as it is initiated, scores it, and publishes the result to `digitalpayments.fraud.score`. Its SLA is lag under 50 ms.

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.lifecycle.v2 \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group digitalpayments-fraud-scoring \
  --property max.poll.records=100 \
  --property session.timeout.ms=10000 \
  --property heartbeat.interval.ms=3000 \
  --property auto.offset.reset=latest \
  --property enable.auto.commit=false \
  --property auto.commit.interval.ms=5000 \
  --property max.poll.interval.ms=30000 \
  --property fetch.min.bytes=1 \
  --property fetch.max.wait.ms=10 \
  --property max.partition.fetch.bytes=1048576 \
  --property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

| Setting | Value | Reason |
|---|---|---|
| Consumer group | `digitalpayments-fraud-scoring` | Hyphen-separated and descriptive. It names the use case (real-time fraud scoring). |
| `max.poll.records` | `100` | Scoring is per payment and must stay under 50 ms. At about 1 KB per event, a poll of 100 records is about 100 KB. That keeps each scoring batch short and the lag low. |
| `session.timeout.ms` | `10000` | Fast failover. If a scoring instance dies, another takes over its partitions within about 10 s. A slow default would leave payments unscored for longer than the 50 ms SLA allows. |
| `heartbeat.interval.ms` | `3000` | One-third of the session timeout, as Kafka recommends. |
| `auto.offset.reset` | `latest` | A new scoring group should score new payments, not replay years of history. Replaying old payments would be wrong for real-time decisions. The archive group covers history. |
| `enable.auto.commit` | `false` | The offset is committed only after the score is published, so a crash cannot skip a payment. The score is then at-least-once, and a duplicate score is removed by `payment_id`. |
| `auto.commit.interval.ms` | `5000` | Has no effect while auto-commit is off. It is set only to keep the template complete. |
| `max.poll.interval.ms` | `30000` | Scoring 100 events must finish within 30 s. A longer limit would hide a stuck scorer. |
| `fetch.min.bytes` | `1` | Return as soon as any data is available. This is the lowest-latency setting. |
| `fetch.max.wait.ms` | `10` | Caps the broker's wait for a full fetch at 10 ms, so a quiet partition cannot add more than 10 ms of delay on the 50 ms path. |
| `max.partition.fetch.bytes` | `1048576` (1 MiB) | At least 4 times the payment topic's 256 KiB maximum message size, so no payment event fails to fetch. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | Scaling the scorers moves only the partitions that must move, so scoring does not pause across the whole group. |

**Scalability:** this group reads the 100 partitions of `digitalpayments.payment.lifecycle.v2`. At 10 Mb/s per instance, the full 1000 Mb/s peak needs 100 instances. The consumer scales out to 100 and no further, because there is one consumer per partition.

## Producer configs

Producer: the fraud scoring service. It publishes one score per payment, keyed by `payment_id`.

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.fraud.score \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property acks=all \
  --property retries=2147483647 \
  --property max.in.flight.requests.per.connection=5 \
  --property enable.idempotence=true \
  --property compression.type=lz4 \
  --property linger.ms=1 \
  --property batch.size=16384 \
  --property delivery.timeout.ms=10000 \
  --property request.timeout.ms=3000
```

| Setting | Value | Reason |
|---|---|---|
| `acks` | `all` | A fraud score is a regulated record and must not be lost. It is the record that justifies a block or an approval. |
| `retries` | `2147483647` | Retry until the delivery timeout. The timeout, not the attempt count, bounds the delay. |
| `max.in.flight.requests.per.connection` | `5` | The highest value that keeps order under idempotence. It keeps a payment's scores in sequence. |
| `enable.idempotence` | `true` | A retried send after a timeout does not produce a duplicate score. |
| `compression.type` | `lz4` | Matches the topic. Lowest CPU cost, which suits the 50 ms path. |
| `linger.ms` | `1` | Waits at most 1 ms to batch. Scores are time-critical, so this stays near zero. Batching still happens under load. |
| `batch.size` | `16384` (16 KiB) | Kafka's default. Scores are small, so a 16 KiB batch fills quickly at this volume without holding events back. |
| `delivery.timeout.ms` | `10000` | Fraud scores must reach Kafka within 10 s. Beyond that the score is stale, and the payment decision has to fall back to a rule-based default. |
| `request.timeout.ms` | `3000` | One send attempt gives up after 3 s, well under the 10 s delivery timeout. It leaves room for several retries inside the window. |

**Partition key:** `payment_id`. Every score for a payment stays on the same partition as that payment's lifecycle events.

## Notes on the commands above

- The consumer and producer commands use `--property` for client settings. The Kafka console tools accept client settings only through `--consumer-property` / `--producer-property` or `--consumer.config` / `--producer.config`. As written, the consumer and producer tuning is ignored. Before real use, change the flags to `--consumer-property` and `--producer-property`.
- Keep values glued to their keys (`retention.ms=220752000000`, not `retention.ms= 220752000000`). The space in the template would split the value into a separate argument.

## Test run

- **Create:** the first attempt timed out after about 60 s (`createTopics` call disconnected from `kafka-0`). A `--list` check showed the topic had not been created, so the create was retried.
- **Retry:** `Created topic digitalpayments.fraud.score.`
- **Describe:** `PartitionCount: 100`, `ReplicationFactor: 3`, `Configs: compression.type=lz4, min.insync.replicas=2, cleanup.policy=delete, retention.ms=220752000000, max.message.bytes=65536`. This matches the design above.
- **Not run:** the console producer and consumer. No mock fraud scores were produced.
