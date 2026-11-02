# Payment producer, topic/s and event/s

## Topic configs

Topic: `digitalpayments.payment.lifecycle.v2`

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digitalpayments.payment.lifecycle.v2 \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220752000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=262144
```

| Setting | Value | Reason |
|---|---|---|
| `--topic` | `digitalpayments.payment.lifecycle.v2` | Follows the `<domain>.<entity>.<event>` convention already used on this cluster. The name describes the payment lifecycle events this topic carries (initiated, authorised, validated, completed, and their failure states). The `.v2` suffix marks this as the new design. It also avoids the original `digitalpayments.payment.lifecycle` name, which the cluster keeps re-creating with a default layout (see Test run). |
| `--partitions` | `100` | The brief sets a 1000 Mb/s peak and a 10 Mb/s per-consumer ceiling, so one consumer group needs 1000 / 10 = 100 consumers to keep up. Partitions cap a group's parallelism, so 100 lets the fraud, notification and audit groups each scale to 100 instances. Producers need 1000 / 100 = 10 instances at the 100 Mb/s producer assumption. 100 partitions across 3 brokers is about 33 leaders per broker, which is a normal load. The ratio holds whether "Mb" means bits or bytes. |
| `--replication-factor` | `3` | The cluster has 3 brokers (`kafka-0`, `kafka-1`, `kafka-2`). With 3 replicas, one broker can fail and no payment event is lost. Two replicas would leave no margin for a failure during maintenance. |
| `retention.ms` | `220752000000` | 7 years, per the regulatory hold: 2555 days × 86,400,000 ms. **Capacity risk:** holding 7 years of payment events in Kafka brokers is expensive. Use tiered storage or an archive sink for the cold tail (see the audit consumer below). |
| `cleanup.policy` | `delete` | Payment events are immutable facts, and the topic is an event stream, not a state store. Log compaction would discard earlier lifecycle states that audit needs. |
| `min.insync.replicas` | `2` | With producers on `acks=all`, a write is acknowledged only after 2 of the 3 replicas hold it. The write survives the loss of any one broker, and the cluster still accepts payments when one broker is down. |
| `compression.type` | `zstd` | Payment events are JSON text, which compresses well. Over a 7-year hold, storage is the dominant cost. zstd gives a better ratio than lz4 at a small CPU cost. That cost is well inside the 150 ms payment budget. |
| `max.message.bytes` | `262144` (256 KiB) | A payment event is a few KB. The 256 KiB limit is far above that and stops a malformed or oversized message from hurting broker memory and replication. It also has to be ≤ the consumer `max.partition.fetch.bytes`. |

## Consumer configs

Consumer group: `digitalpayments-audit-archive`. This is the audit and reconciliation consumer for the 7-year hold. It reads every payment lifecycle event from the beginning and writes it to the archive. Its SLA is lag under 1 hour, so it favours throughput over latency.

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.lifecycle.v2 \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group digitalpayments-audit-archive \
  --property max.poll.records=500 \
  --property session.timeout.ms=45000 \
  --property heartbeat.interval.ms=15000 \
  --property auto.offset.reset=earliest \
  --property enable.auto.commit=false \
  --property auto.commit.interval.ms=5000 \
  --property max.poll.interval.ms=300000 \
  --property fetch.min.bytes=65536 \
  --property fetch.max.wait.ms=500 \
  --property max.partition.fetch.bytes=1048576 \
  --property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

| Setting | Value | Reason |
|---|---|---|
| Consumer group | `digitalpayments-audit-archive` | Hyphen-separated and descriptive, as the brief asks. It names the use case (audit and archive). |
| `max.poll.records` | `500` | Archiving is batch work, so larger batches cut per-record overhead. 500 records of a few KB stay well under memory limits. |
| `session.timeout.ms` | `45000` | An archive consumer is not latency-sensitive. A longer session timeout avoids needless rebalances during a slow archive write. |
| `heartbeat.interval.ms` | `15000` | Kept at one-third of the session timeout, as Kafka recommends. |
| `auto.offset.reset` | `earliest` | A new audit group must cover the full retained history, not only new events, or the archive has gaps. |
| `enable.auto.commit` | `false` | The offset is committed only after the event is written to the archive, so a crash cannot skip an event (at-least-once). Duplicates are removed downstream by event ID. |
| `auto.commit.interval.ms` | `5000` | Has no effect while auto-commit is off. It is set only to keep the template complete. |
| `max.poll.interval.ms` | `300000` | Allows up to 5 minutes between polls, which covers slow archive or storage writes without a false rebalance. |
| `fetch.min.bytes` | `65536` | Batching fetches is efficient for bulk archiving. |
| `fetch.max.wait.ms` | `500` | Caps the wait for a full fetch, so the archive never falls far behind. |
| `max.partition.fetch.bytes` | `1048576` (1 MiB) | At least 4 times the topic's `max.message.bytes`, so no message can fail to fetch. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | Rebalances move only the partitions that must move, so scaling the archive does not stop the whole group. |

## Producer configs

Producer: the payment initiation service. It publishes every payment lifecycle event, keyed by `payment_id`.

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.lifecycle.v2 \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property acks=all \
  --property retries=2147483647 \
  --property max.in.flight.requests.per.connection=5 \
  --property enable.idempotence=true \
  --property compression.type=zstd \
  --property linger.ms=5 \
  --property batch.size=65536 \
  --property delivery.timeout.ms=30000 \
  --property request.timeout.ms=10000
```

| Setting | Value | Reason |
|---|---|---|
| `acks` | `all` | The business cost of losing a payment event is high, so the producer waits for all in-sync replicas. Combined with `min.insync.replicas=2`, an acknowledged event survives a broker failure. |
| `retries` | `2147483647` | Retry until `delivery.timeout.ms` expires rather than giving up after a fixed count. Time, not attempts, bounds how long a payment can be stuck. |
| `max.in.flight.requests.per.connection` | `5` | The highest value that still keeps order when idempotence is on. Higher values could reorder events for one payment. |
| `enable.idempotence` | `true` | A network retry after a timeout does not create a duplicate event. The broker de-duplicates on producer ID and sequence number. |
| `compression.type` | `zstd` | Matches the topic setting, so the broker does not recompress batches. |
| `linger.ms` | `5` | Waits up to 5 ms to fill batches. That adds at most 5 ms to the 150 ms payment budget and gives much better throughput at 100 Mb/s per producer. |
| `batch.size` | `65536` (64 KiB) | Up to 4 times the default, so batches fill during bursts. The value is fixed by the throughput target, not by latency. |
| `delivery.timeout.ms` | `30000` | Bounds how long a payment event can be stuck in the producer to 30 s before it fails. That is long enough to ride out a leader election, and short enough that the customer gets a clear outcome. |
| `request.timeout.ms` | `10000` | A single send attempt gives up after 10 s. This must be less than `delivery.timeout.ms` (30 s) and greater than `linger.ms` plus broker latency. |

**Partition key:** `payment_id`. All events for one payment (initiated, authorised, validated, completed) hash to the same partition, so they stay in order for the fraud, notification and audit consumers. Event order across different payments does not matter.

**Mock data volume:** 9 events across 3 payment journeys. See the Test run section below.

## Notes on the commands above

- The consumer and producer commands use `--property` for client settings. The Kafka console tools accept client settings only through `--consumer-property` / `--producer-property` or `--consumer.config` / `--producer.config`. As written, the `--property` settings for `acks`, `retries`, and the consumer tuning are ignored. Before real use, the tools need the flags changed to `--producer-property` and `--consumer-property`.
- Keep values glued to their keys (`retention.ms=220752000000`, not `retention.ms= 220752000000`). The space in the template would split the value into a separate argument.

## Test run

The original topic `digitalpayments.payment.lifecycle` was not usable: it had 1 partition and replication factor 1, and the cluster kept re-creating it with the default layout after each delete (broker auto-create is on, and kafka-ui polls topic metadata). The design was therefore created under the new name `digitalpayments.payment.lifecycle.v2`.

**Create**

```bash
kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --create \
  --topic digitalpayments.payment.lifecycle.v2 --partitions 100 --replication-factor 3 \
  --config retention.ms=220752000000 --config cleanup.policy=delete \
  --config min.insync.replicas=2 --config compression.type=zstd --config max.message.bytes=262144
```

Output: `Created topic digitalpayments.payment.lifecycle.v2.`

**Describe**

```text
Topic: digitalpayments.payment.lifecycle.v2  PartitionCount: 100  ReplicationFactor: 3  Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=262144
```

**Produce:** 9 events across 3 payment journeys, sent with `acks=all`, idempotence on, and zstd. Each event is keyed by `payment_id`.

- PAY-1001: initiated → authorised → validated → completed (completed journey)
- PAY-1002: initiated → not authorised, reason `INSUFFICIENT_FUNDS` (failed journey)
- PAY-1003: initiated → authorised → invalidated, reason `ACCOUNT_CLOSED` (invalidated journey)

The producer command used the real client settings (`--producer-property`), not the template's `--property`. The tool warns that `--producer-property` and `--property` are deprecated in this Kafka version, but the run succeeded.

**Verify (consume from beginning):** all 9 events came back.

| payment_id | Partition | Offsets | Order kept |
|---|---|---|---|
| PAY-1001 | 59 | 0–3 | yes |
| PAY-1002 | 59 | 4–5 | yes |
| PAY-1003 | 82 | 0–2 | yes |

Each payment's events stay on one partition in sequence, which is what the partition key is meant to do. PAY-1001 and PAY-1002 share partition 59 because they hash to the same partition, which is normal with 100 partitions.

**Not run:** the consumer groups and their lag. No consumer is running, so lag was not measured.
