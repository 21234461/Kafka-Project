# Notification producer, consumer, topic/s and event/s

## Topic configs

Topic: `digitalpayments.notification.events`. The notification producer publishes the payment approved and payment failed events here.

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digitalpayments.notification.events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=65536
```

| Setting | Value | Reason |
|---|---|---|
| `--topic` | `digitalpayments.notification.events` | Same `<domain>.<entity>.<event>` convention. It names the notification events this topic carries. |
| `--partitions` | `100` | Sized for the worst case: every payment produces a notification, so the topic could carry the full 1000 Mb/s peak. That needs 1000 / 10 = 100 consumers. It also matches the payment and fraud topics, so one `payment_id` maps to the same partition index in all three. |
| `--replication-factor` | `3` | A lost notification means a customer is not told that a payment succeeded or failed. The cluster has 3 brokers, so 3 replicas survive one broker failure. |
| `retention.ms` | `604800000` | 7 days (7 × 86,400,000 ms). Notifications are not a regulated record. The 7-day window covers replaying failed sends and customer queries, while the 7-year record lives in the payment and fraud topics. A shorter window also keeps storage small. |
| `cleanup.policy` | `delete` | A notification is a one-off message, not a state. Compaction would make no sense for it. |
| `min.insync.replicas` | `2` | With `acks=all`, a notification is acknowledged only once 2 replicas hold it. One broker can fail without losing a notification. |
| `compression.type` | `zstd` | Notification text compresses well. The topic has a 2 s SLA, which is far looser than the fraud path's 50 ms, so the better compression ratio is worth the small CPU cost. |
| `max.message.bytes` | `65536` (64 KiB) | A notification holds a payment ID, a status, a channel, and a short message. 64 KiB is more than enough and stops oversized content, such as a bulk template, from reaching the topic. |

## Consumer configs

Consumer group: `digitalpayments-notification-sender`. This consumer reads the payment failed and payment validated events from the payment topic, and sends customer notifications. Its SLA is lag under 2 s.

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.lifecycle.v2 \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group digitalpayments-notification-sender \
  --property max.poll.records=50 \
  --property session.timeout.ms=30000 \
  --property heartbeat.interval.ms=10000 \
  --property auto.offset.reset=latest \
  --property enable.auto.commit=false \
  --property auto.commit.interval.ms=5000 \
  --property max.poll.interval.ms=300000 \
  --property fetch.min.bytes=1024 \
  --property fetch.max.wait.ms=500 \
  --property max.partition.fetch.bytes=1048576 \
  --property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

| Setting | Value | Reason |
|---|---|---|
| Consumer group | `digitalpayments-notification-sender` | Hyphen-separated and descriptive. It names the use case (sending notifications). |
| `max.poll.records` | `50` | Each record triggers an outbound call to an SMS, push or email gateway. Small batches keep one slow send from delaying the rest, and keep a whole batch comfortably inside the 2 s budget. |
| `session.timeout.ms` | `30000` | Gateway calls can be slow. A longer session timeout stops the group from rebalancing and resending notifications when a send runs long. |
| `heartbeat.interval.ms` | `10000` | One-third of the session timeout, as Kafka recommends. |
| `auto.offset.reset` | `latest` | A new sender group must not send old notifications to customers. Replaying a year of events would send stale messages. |
| `enable.auto.commit` | `false` | The offset is committed only after the notification is sent, so a crash cannot drop a notification. The risk is a duplicate send. Sends are de-duplicated by `payment_id` plus event type. |
| `auto.commit.interval.ms` | `5000` | Has no effect while auto-commit is off. It is set only to keep the template complete. |
| `max.poll.interval.ms` | `300000` | Allows 5 minutes per batch. That covers slow third-party gateways without a false rebalance. |
| `fetch.min.bytes` | `1024` | Waits for about 1 KiB before returning. This batches fetches efficiently, and the 500 ms wait below caps the added delay. |
| `fetch.max.wait.ms` | `500` | Caps the fetch wait at 500 ms. Combined with processing time, this keeps the 2 s SLA with headroom. |
| `max.partition.fetch.bytes` | `1048576` (1 MiB) | At least 16 times the topic's 64 KiB maximum message size. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | Scaling the senders moves only the partitions that must move, so customer notifications keep flowing during a deployment. |

**Scalability:** this group can run up to 100 instances, one per partition. Each instance handles about 10 Mb/s.

## Producer configs

Producer: the notification service. It publishes an event for each payment that is approved or failed, keyed by `payment_id`.

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.notification.events \
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
  --property batch.size=32768 \
  --property delivery.timeout.ms=60000 \
  --property request.timeout.ms=10000
```

| Setting | Value | Reason |
|---|---|---|
| `acks` | `all` | Losing a notification means a customer is never told about a payment. The durability cost is small, so the producer waits for all in-sync replicas. |
| `retries` | `2147483647` | Retry until the delivery timeout. The timeout bounds the delay, not the attempt count. |
| `max.in.flight.requests.per.connection` | `5` | The highest value that keeps order under idempotence. A customer receives updates in the right sequence. |
| `enable.idempotence` | `true` | A retried send does not create a duplicate notification event. |
| `compression.type` | `zstd` | Matches the topic. |
| `linger.ms` | `5` | Waits up to 5 ms to batch. That costs almost nothing against a 2 s SLA and raises throughput. |
| `batch.size` | `32768` (32 KiB) | Half the payment producer's batch. Notification events are smaller, and this size fills well at the expected volume. |
| `delivery.timeout.ms` | `60000` | Allows up to 60 s for the event to reach Kafka. A notification is less time-critical than a payment decision, so a longer window is acceptable. |
| `request.timeout.ms` | `10000` | One send attempt gives up after 10 s. It is well under the 60 s delivery timeout and leaves room for several retries. |

**Partition key:** `payment_id`. Each payment's notification events stay on the same partition, in order.

## Notes on the commands above

- The consumer and producer commands use `--property` for client settings. The Kafka console tools accept client settings only through `--consumer-property` / `--producer-property` or `--consumer.config` / `--producer.config`. As written, the consumer and producer tuning is ignored. Before real use, change the flags to `--consumer-property` and `--producer-property`.
- Keep values glued to their keys (`retention.ms=604800000`, not `retention.ms= 604800000`). The space in the template would split the value into a separate argument.

## Test run

- **Create:** `Created topic digitalpayments.notification.events.` on the first attempt.
- **Describe:** `PartitionCount: 100`, `ReplicationFactor: 3`, `Configs: compression.type=zstd, min.insync.replicas=2, cleanup.policy=delete, retention.ms=604800000, max.message.bytes=65536`. Every partition lists 3 replicas in the ISR.
- **Not run:** the console producer and consumer. No mock notification events were produced.
