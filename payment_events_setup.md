# Payment Events Setup: Kafka POC

**Project:** Digital Payments Event Streaming Infrastructure
**Team:** Digital Payments Engineering

This document covers the design, setup, execution and verification of the payment, fraud and notification event streams. Each design decision is justified against the requirements in `project.md`. The detailed per-topic and per-group settings are in [payment.md](payment.md), [fraud.md](fraud.md) and [notification.md](notification.md). The scenario and live-run evidence is in `scenarios/`.

---

## 1. Design Decisions

### 1.1 Cluster

- Three KRaft brokers in the StatefulSet `kafka` (`kafka-0`, `kafka-1`, `kafka-2`), reached through the service `kafka-service:9092`.
- Hosted on a local Kubernetes cluster (Rancher Desktop, context `rancher-desktop`). The cluster was provided, so I did not start it. To check it is up: `kubectl get pods` should show `kafka-0`, `kafka-1` and `kafka-2` as `Running`.

### 1.2 Topics

| Topic | Purpose | Partitions | RF | Min ISR | Retention | Cleanup | Compression | Max message |
|---|---|---|---|---|---|---|---|---|
| `digitalpayments.payment.lifecycle.v2` | Payment lifecycle events | 100 | 3 | 2 | 7 years (220752000000 ms) | delete | zstd | 256 KiB |
| `digitalpayments.fraud.score` | Fraud scores (low, medium, high) | 100 | 3 | 2 | 7 years | delete | lz4 | 64 KiB |
| `digitalpayments.notification.events` | Approved and failed notifications | 100 | 3 | 2 | 7 days (604800000 ms) | delete | zstd | 64 KiB |

**Topic name.** The names follow the `<domain>.<entity>.<event>` convention already used on the cluster. The payment topic carries a `.v2` suffix. The original `digitalpayments.payment.lifecycle` topic exists with a different layout and is still being re-created by the broker (see Section 7, issue 1).

**Partitions: 100 on every topic.** The brief sets a 1000 Mb/s peak and 10 Mb/s per consumer instance. One consumer group therefore needs 1000 / 10 = 100 consumers, and a partition can be read by only one consumer in a group, so 100 partitions allow 100-way parallelism. Producers need 1000 / 100 = 10 instances at the 100 Mb/s assumption. Using the same count on all three topics means Kafka's default partitioner maps one `payment_id` to the same partition index on each topic. The ratio is the same whether "Mb" means bits or bytes.

**Replication factor 3 and min ISR 2.** The cluster has three brokers. With producers on `acks=all`, a write is acknowledged only after two replicas hold it, so no acknowledged event is lost if one broker fails. The cluster keeps accepting writes while one broker is down.

**Retention: 7 years for payment and fraud, 7 days for notifications.** The brief requires a 7-year hold for payment and fraud events, which is 2555 days (220752000000 ms). Notifications are not a regulated record, and their 7-day window covers replay of failed sends. Storage is the main risk of the 7-year hold: at peak throughput, keeping 7 years in broker storage is not practical. The correct answer is tiered storage or an archive sink for older data. The audit consumer (Section 3.2) is the first step towards that. Size-based retention is not set, because deleting by size would silently break the 7-year hold.

**Cleanup policy: `delete`, not compaction.** Payment and fraud events are immutable facts, and the audit trail needs every lifecycle state. Compaction keeps only the latest value per key, which would discard the history.

**Compression: zstd for payment and notification, lz4 for fraud.** Payment events are JSON text, which compresses well. The 7-year storage cost dominates, and zstd gives the better ratio. The fraud path has the tightest budget (lag under 50 ms), so lz4 is used there for its lower CPU cost.

**Message size limits.** Payment events are a few KB, so 256 KiB is far above the normal size but still stops oversized messages. Fraud scores and notifications are under 1 KB, so 64 KiB is enough.

### 1.3 Message schema

Payment lifecycle event (JSON, key = `payment_id`):

| Field | Type | Description |
|---|---|---|
| `eventId` | string | Unique event ID. Used to de-duplicate consumers. |
| `paymentId` | string | Payment ID. Also the message key. |
| `eventType` | string | One of the event types in Section 1.4. |
| `amount` | number | Amount, on `PAYMENT_INITIATED` only. |
| `currency` | string | ISO currency code (ZAR), on `PAYMENT_INITIATED` only. |
| `channel` | string | MOBILE, WEB or USSD, on `PAYMENT_INITIATED` only. |
| `recentTxnCount1h` | integer | Payments by this customer in the last hour. An input feature for fraud. |
| `reason` | string | Failure reason, on failure events. |
| `timestamp` | string | ISO-8601 time of the event. |

Fraud score event (key = `payment_id`): `eventId`, `paymentId`, `eventType` (`FRAUD_SCORE`), `score` (0–100), `band` (LOW, MEDIUM or HIGH), `rules` (rules that fired), `sourceTimestamp`.

Notification event (key = `payment_id`): `eventId`, `paymentId`, `eventType` (`NOTIFICATION_APPROVED` or `NOTIFICATION_FAILED`), `channel` (SMS), `message`, `sourceEvent`.

**Why JSON.** JSON is readable in the console tools used for this POC, and it needs no schema infrastructure. Avro or Protobuf would be smaller and would enforce a schema, which matters for production. The cluster has no Schema Registry, so JSON was the practical choice. Schema evolution is a known gap (see Section 6).

### 1.4 Producer design

**Event types published by the payment producer:**

```
PAYMENT_INITIATED
  -> PAYMENT_AUTHORISED -> PAYMENT_VALIDATED -> PAYMENT_COMPLETED
  -> PAYMENT_NOT_COMPLETED
  -> PAYMENT_INVALIDATED
  -> PAYMENT_NOT_AUTHORISED
```

Every state change is published, so a payment can be rebuilt from its events.

**Partition key: `payment_id`.** All events for one payment hash to the same partition, so they reach the consumers in order. Order between different payments does not matter, so keying by payment gives ordering where it is needed and parallelism everywhere else.

**Durability: `acks=all`.** The business cost of losing a payment event is high, so the producer waits until all in-sync replicas have the write. The 150 ms payment budget has room for the extra round trip.

**Idempotence: `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`.** A network retry after a timeout cannot create a duplicate within one producer session, and 5 in-flight requests is the highest value that still keeps order. Idempotence does not protect against a second send from a *new* producer run. Section 7, issue 5 shows this happening.

**Error handling: retry until `delivery.timeout.ms` = 30 s.** `retries` is set to its maximum, so the producer retries until the 30 s deadline. A payment can be stuck in the producer for at most 30 s before it fails back to the caller. There is no dead-letter queue in this POC. A failed send is reported to the caller, which is responsible for retrying or raising an alert.

**Mock volume:** 59 events across 14 payment flows were produced in total, including the duplicated live batch (see Section 3.1).

---

## 2. Topic Creation

### 2.1 Commands

```bash
kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --create \
  --topic digitalpayments.payment.lifecycle.v2 --partitions 100 --replication-factor 3 \
  --config retention.ms=220752000000 --config cleanup.policy=delete \
  --config min.insync.replicas=2 --config compression.type=zstd --config max.message.bytes=262144

kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --create \
  --topic digitalpayments.fraud.score --partitions 100 --replication-factor 3 \
  --config retention.ms=220752000000 --config cleanup.policy=delete \
  --config min.insync.replicas=2 --config compression.type=lz4 --config max.message.bytes=65536

kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --create \
  --topic digitalpayments.notification.events --partitions 100 --replication-factor 3 \
  --config retention.ms=604800000 --config cleanup.policy=delete \
  --config min.insync.replicas=2 --config compression.type=zstd --config max.message.bytes=65536
```

### 2.2 Results

- `digitalpayments.payment.lifecycle.v2`: created on the first attempt.
- `digitalpayments.fraud.score`: the first attempt timed out. A list check showed it had not been created, and the retry succeeded.
- `digitalpayments.notification.events`: created on the first attempt.

### 2.3 Describe output

Full output is saved in `scenarios/evidence/`. Headers are shown here.

```
Topic: digitalpayments.payment.lifecycle.v2  PartitionCount: 100  ReplicationFactor: 3
  Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=262144

Topic: digitalpayments.fraud.score           PartitionCount: 100  ReplicationFactor: 3
  Configs: compression.type=lz4,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=65536

Topic: digitalpayments.notification.events   PartitionCount: 100  ReplicationFactor: 3
  Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,retention.ms=604800000,max.message.bytes=65536
```

Evidence files: `scenarios/evidence/describe-digitalpayments.payment.lifecycle.v2.txt`, `describe-digitalpayments.fraud.score.txt`, `describe-digitalpayments.notification.events.txt`.

---

## 3. Producer Setup

### 3.1 Events produced

| Batch | Payments | Events | Purpose |
|---|---|---|---|
| Demo flows | PAY-1001, PAY-1002, PAY-1003 | 9 | Basic journeys: completed, not authorised, invalidated |
| Scenarios | 7 (SCN-*) | 23 | Fraud bands and notification outcomes |
| Live batch 1 | LIVE-COMP-001, LIVE-HIGH-001, LIVE-FAIL-002 | 9, sent **twice** = 18 | Live run, see issue 5 |
| Live batch 2 | LIVE2-COMP-001, LIVE2-HIGH-001, LIVE2-FAIL-002 | 9 | Clean live run with the three consumer groups |
| **Total on the payment topic** | | **59** | Confirmed by the topic dump |

### 3.2 Producer command

The producer is the console producer, run through the scripts in `scenarios/`. It uses the designed client settings:

```bash
kafka-console-producer --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.lifecycle.v2 \
  --property parse.key=true --property key.separator=: \
  --producer-property acks=all --producer-property enable.idempotence=true \
  --producer-property compression.type=zstd --producer-property linger.ms=5 \
  --producer-property delivery.timeout.ms=30000 --producer-property request.timeout.ms=10000
```

Scripts: [run_scenarios.sh](scenarios/run_scenarios.sh) and [run_live_consumers.sh](scenarios/run_live_consumers.sh). Event data: [scenario_events.txt](scenarios/scenario_events.txt), [live_events.txt](scenarios/live_events.txt) and [live_events_run2.txt](scenarios/live_events_run2.txt).

**Note on the template.** The `--property` flags in the template for client settings are not applied by the console tools. The `--producer-property` form above is what was actually used.

### 3.3 Example events

Payment lifecycle (key `LIVE2-COMP-001`, partition 18):

```json
{"eventId":"L2-COMP-001-1","paymentId":"LIVE2-COMP-001","eventType":"PAYMENT_INITIATED","amount":1500.00,"currency":"ZAR","channel":"MOBILE","recentTxnCount1h":0,"timestamp":"2026-11-02T09:00:00Z"}
{"eventId":"L2-COMP-001-4","paymentId":"LIVE2-COMP-001","eventType":"PAYMENT_COMPLETED","timestamp":"2026-11-02T09:00:00.160Z"}
```

Failed payment (key `LIVE2-HIGH-001`, partition 20):

```json
{"eventId":"L2-HIGH-001-2","paymentId":"LIVE2-HIGH-001","eventType":"PAYMENT_NOT_AUTHORISED","reason":"FRAUD_HIGH_RISK","timestamp":"2026-11-02T09:01:00.120Z"}
```

Fraud score (produced by the fraud group):

```json
{"eventId":"FS-LIVE2-HIGH-001","paymentId":"LIVE2-HIGH-001","eventType":"FRAUD_SCORE","score":90,"band":"HIGH","rules":"HIGH_AMOUNT,LOW_ASSURANCE_CHANNEL,VELOCITY_BREACH"}
```

Notification (produced by the notification group):

```json
{"eventId":"NS-LIVE2-FAIL-002-PAYMENT_INVALIDATED","paymentId":"LIVE2-FAIL-002","eventType":"NOTIFICATION_FAILED","channel":"SMS","message":"Your payment LIVE2-FAIL-002 failed: ACCOUNT_CLOSED","sourceEvent":"PAYMENT_INVALIDATED"}
```

### 3.4 Evidence of production

- **Offsets and partitions:** the payment topic has 59 messages across 14 partitions. The per-partition offsets are in `scenarios/evidence/payment-v2-offsets-nonzero.txt`, and the full dump is in `scenarios/evidence/payment-v2-all-events.txt`.
- **Partition keys:** each payment's events are on one partition with consecutive offsets. For example, LIVE2-COMP-001 has offsets 0–3 on partition 18, and LIVE2-FAIL-002 has offsets 0–2 on partition 91.

---

## 4. Consumer Groups

Three independent consumer groups were built, one per downstream use case. Each reads the payment topic with its own group ID and commits offsets manually after processing (at-least-once).

| Group | Purpose | Reads | Start | Lag SLA | Output |
|---|---|---|---|---|---|
| `digitalpayments-audit-archive` | Audit and reconciliation archive | All payment lifecycle events | `earliest` | < 1 hour | Archive record per event |
| `digitalpayments-fraud-scoring` | Real-time fraud scoring | `PAYMENT_INITIATED` | `latest` | < 50 ms | `FRAUD_SCORE` on `digitalpayments.fraud.score` |
| `digitalpayments-notification-sender` | Customer notifications | `PAYMENT_VALIDATED`, `NOT_AUTHORISED`, `INVALIDATED`, `NOT_COMPLETED` | `latest` | < 2 s | `NOTIFICATION_APPROVED` / `_FAILED` on `digitalpayments.notification.events` |

The full settings for each group, with the reason for each value, are in [payment.md](payment.md) (audit), [fraud.md](fraud.md) (fraud) and [notification.md](notification.md) (notifications). Key choices:

- **Manual offset commit** (`enable.auto.commit=false`): a crash cannot skip an event. A redelivered event may produce a duplicate, which consumers remove by `paymentId` and event type.
- **Start position:** the audit group starts from the beginning, so the archive has no gaps. The real-time groups start from `latest`, so a new deployment does not re-score or re-notify old payments.
- **Cooperative sticky assignment:** a rebalance moves only the partitions that must move, so scaling does not pause a whole group.
- **Scalability:** each group can run up to 100 instances (one per partition). At 10 Mb/s per instance, 100 instances cover the 1000 Mb/s peak.

### 4.1 How the consumers were run

The console consumer cannot commit offsets manually, and the design needs manual commits. Each group was therefore built as a small Java program, [GroupConsumer.java](scenarios/GroupConsumer.java), which uses the Kafka client library in the broker image. It applies the settings from the three design files. The fraud and notification modes also publish their results back to Kafka.

The program was compiled on a JDK 21 machine and the class file was copied into the pod, because the pod's Java runtime has no compiler.

Start command for the live run, from [run_live_consumers.sh](scenarios/run_live_consumers.sh):

```bash
java -cp '/tmp/gc:/usr/share/java/kafka/*' GroupConsumer audit digitalpayments-audit-archive 240
java -cp '/tmp/gc:/usr/share/java/kafka/*' GroupConsumer fraud digitalpayments-fraud-scoring 240
java -cp '/tmp/gc:/usr/share/java/kafka/*' GroupConsumer notification digitalpayments-notification-sender 240
```

### 4.2 Sample output

Fraud group (live batch 2):

```
SCORED p18@0 LIVE2-COMP-001 score=0 band=LOW rules=NONE
SCORED p91@0 LIVE2-FAIL-002 score=5 band=LOW rules=WEB_CHANNEL
SCORED p20@0 LIVE2-HIGH-001 score=90 band=HIGH rules=HIGH_AMOUNT,LOW_ASSURANCE_CHANNEL,VELOCITY_BREACH
DONE mode=fraud group=digitalpayments-fraud-scoring processed=9
```

Notification group (live batch 2):

```
NOTIFIED p91@2 LIVE2-FAIL-002 NOTIFICATION_FAILED (from PAYMENT_INVALIDATED)
NOTIFIED p18@2 LIVE2-COMP-001 NOTIFICATION_APPROVED (from PAYMENT_VALIDATED)
NOTIFIED p20@1 LIVE2-HIGH-001 NOTIFICATION_FAILED (from PAYMENT_NOT_AUTHORISED)
DONE mode=notification group=digitalpayments-notification-sender processed=9
```

Audit group (live batch 2, the events not yet committed by the earlier run):

```
ARCHIVED p18@0 LIVE2-COMP-001 PAYMENT_INITIATED
ARCHIVED p18@1 LIVE2-COMP-001 PAYMENT_AUTHORISED
...
DONE mode=audit group=digitalpayments-audit-archive processed=27
```

### 4.3 Filtering and transformations

- The fraud group drops every event except `PAYMENT_INITIATED`.
- The notification group drops every event except the four states that produce a notification. Each approved notification is sent on validation, not completion, so a customer is told once.
- The audit group does no filtering and writes every event.

### 4.4 Error handling

- A failed fraud or notification publish throws, so its offset is not committed and the event is retried on the next poll.
- The audit group archives every event, so it has no filtering to fail.
- **Not tested:** a consumer crash mid-batch and the resulting redelivery. The manual-commit design is in place, but no failure was injected.

---

## 5. Verification and Monitoring

### 5.1 Consumer group status

Captured during the live run (`scenarios/live_run/describe-*.txt`):

| Group | Partitions with messages | Total LAG |
|---|---|---|
| `digitalpayments-audit-archive` | 14 | 0 |
| `digitalpayments-fraud-scoring` | 100 (all committed) | 0 |
| `digitalpayments-notification-sender` | 100 (all committed) | 0 |

The audit group showed an active member (`consumer-digitalpayments-audit-archive-1`) in the describe output. The fraud and notification groups showed committed offsets but **no active member** at describe time, although their logs show they were running and processed 9 events each. I have not found the cause.

### 5.2 Lag

- At describe time, lag was 0 on every partition for all three groups.
- The audit group's committed offsets add up to 59, the topic's total message count, so every event has been archived. This run logged 27 archived events. The other 32 were archived by the earlier live run, whose log was overwritten. That split is inferred from the counts, not logged directly.
- End-to-end latency against the business targets (150 ms payment, 50 ms fraud, 2 s notification) was **not measured**.

### 5.3 Topic status

All three topics show `ReplicationFactor: 3` with every partition in sync. The full describe output is in `scenarios/evidence/`.

### 5.4 Ordering verification

Related events stay together:

- **LIVE2-COMP-001:** partition 18, offsets 0 → 3, in order: `INITIATED`, `AUTHORISED`, `VALIDATED`, `COMPLETED`.
- **LIVE2-FAIL-002:** partition 91, offsets 0 → 2, in order: `INITIATED`, `AUTHORISED`, `INVALIDATED`.
- **LIVE2-HIGH-001:** partition 20, offsets 0 → 1, in order: `INITIATED`, `NOT_AUTHORISED`.
- **Earlier batches:** PAY-1001 (partition 59, offsets 0–3), PAY-1002 (partition 59, offsets 4–5) and PAY-1003 (partition 82, offsets 0–2) also keep their order.

PAY-1001 and PAY-1002 share partition 59. Two keys can hash to one partition, but each key's events stay in order.

**Duplicate events:** the `LIVE-*` batch is on the topic twice (partition 26 holds copy one at offsets 0–3 and copy two at offsets 4–7, with the same `eventId`s). Order within each copy is still correct, but the duplicates are real data on the topic. See issue 5.

### 5.5 Scenario results

Full results are in [scenario_results.md](scenarios/scenario_results.md). Summary:

- 7 scenarios produced fraud scores from 0 to 90, in the LOW, MEDIUM and HIGH bands.
- 11 notifications were sent (4 approved, 7 failed) across the demo and scenario batches.

---

## 6. Trade-offs and Justifications

| Decision | Chosen | Alternative | Why this one |
|---|---|---|---|
| Partitions | 100 | 10, 30, 50 | Matches the 100-consumer ceiling for the 1000 Mb/s peak. Costs more partition overhead, which the 3-broker cluster can carry. |
| Replication | 3, min ISR 2 | 2, or min ISR 3 | Survives one broker failure without data loss or an outage. Min ISR 3 would stop writes whenever a broker is down. |
| Retention | 7 years on payment and fraud | Size-based, or a shorter window | The regulatory hold requires 7 years. Size limits would break it silently. The storage cost is high and needs tiered storage (not yet built). |
| Cleanup | delete | compact | Events are facts, and the audit needs every state. |
| Compression | zstd / lz4 | none, snappy | zstd gives the best storage saving for 7-year data. lz4 keeps CPU low on the 50 ms path. |
| Producer acks | all | 1, or 0 | No acknowledged event may be lost. The extra latency is well within the 150 ms budget. |
| Idempotence | on | off | Stops duplicates from client retries within a session. Does not stop duplicates from a new producer run (issue 5). |
| Serialisation | JSON | Avro, Protobuf | Readable, needs no schema registry (none on this cluster). Gives up size and schema enforcement. |
| Consumer commits | manual | auto-commit | No event is skipped on a crash. Duplicates are handled by the consumer. |
| Consumer implementation | Java client | console consumer | The console consumer cannot commit manually. The Java program is more code but meets the design. |
| Fraud enforcement | score published, not enforced | block on score | Keeps authorisation fast. The trade-off is that a HIGH score does not stop a payment by itself (open point, Section 7). |

---

## 7. Issues Encountered

1. **The original payment topic was wrong and kept returning.** `digitalpayments.payment.lifecycle` existed with 1 partition and RF 1, and min ISR 2, so `acks=all` writes would be rejected. Deleting it worked, but the broker re-created it with the default layout within about a second. Broker auto-create (`auto.create.topics.enable=true`) is on, and kafka-ui polls topic metadata. Each delete-then-create attempt lost the race. **Resolution:** the design was created under the new name `digitalpayments.payment.lifecycle.v2`. **Still open:** the old topic was deleted again during the scope cleanup (it held 0 messages), and it came back within a minute. The likely cause is `payment-consumer-group`, which has an active consumer subscribed to the old topic name. Each subscription triggers broker auto-create. Stopping that consumer is outside this project and needs the owner's approval.

2. **`kubectl exec -i` hung on non-producer commands.** Commands that do not read stdin hang when stdin is left open. **Fix:** `< /dev/null` on those commands. The producer still needs stdin.

3. **The console tools ignored the template's client settings.** `--property` does not apply client configs. **Fix:** `--producer-property` for the producer. Templates still need correcting.

4. **The console consumer cannot commit offsets manually.** The design needs manual commits. **Fix:** a Java consumer program, `GroupConsumer.java`.

5. **Duplicate events.** The first `LIVE-*` produce timed out with an unknown result, and the retry sent the batch again. The topic now holds two copies, with different offsets but the same `eventId`s. Idempotence stops duplicates only within one producer session. **Mitigation in the design:** consumers de-duplicate by `eventId`. Removing the first copy of each duplicate (a prefix delete on partitions 26, 43 and 97) was attempted and denied by the permission system, so the topic still holds 59 events, with 9 duplicated. Removal needs your approval.

6. **Slow cluster.** The KRaft controller logged slow controller events and rejected votes, and a topic list took 25 s. Admin calls timed out and had to be retried.

7. **Scenario reruns duplicate data.** `run_scenarios.sh` re-produces its events each time, and the fraud and notification topics are not de-duplicated. **Fix:** `SKIP_PRODUCE=1` reuses the events already on the topic.

8. **A 15-second consumer run processed 0 events.** The consumer needed more than 15 s to join. A 60-second run worked.

9. **Fraud and notification groups showed no active member** in the describe output while their logs show them running (Section 5.1). Not yet explained.

10. **An unknown consumer group exists.** `payment-consumer-group` is on the cluster but is not part of this design. It has an active `console-consumer` member on the old topic (`digitalpayments.payment.lifecycle`), so it cannot be deleted without stopping that member. Its owner is unknown.

11. **Template typo.** The templates put a space after `=` (`retention.ms= <value>`). Written as one argument, the value would be treated as a separate argument. **Fix:** no space in the values.

---

## 8. Open Gaps

These items from `project.md` are not complete:

- **Throughput and latency were not tested.** The 1000 Mb/s peak and the 150 ms / 50 ms / 2 s targets were not load-tested or measured end to end.
- **No dead-letter queue.** Failed events are reported to the caller and not routed to a DLQ, so project section 3.3 (error handling and DLQ) is answered by design only.
- **No failure test.** A consumer crash and the resulting redelivery were not injected.
- **Fraud and notification describe output** does not show an active member (issue 9).
- **`partition.assignment.strategy`** is set on all groups. It may be ignored by the new consumer group protocol, which the Java client uses by default. Not verified.
- **Retention capacity** for 7 years was not sized, and tiered storage is not set up.
- **Schema evolution** is not handled, because there is no Schema Registry.
- **Out-of-scope items still on the cluster:** the old topic `digitalpayments.payment.lifecycle` (issue 1), the duplicated live records (issue 5), and `payment-consumer-group` (issue 10). Each needs a decision before it can be removed. Temporary `console-consumer-*` groups from ad-hoc dumps expire on their own.

---

## 9. Conclusions

- The design choices are set and justified against the brief: 100 partitions, replication factor 3 with min ISR 2, 7-year retention for payment and fraud, and `acks=all` with idempotence.
- Three independent consumer groups read the same payment stream and do different jobs. Each runs with its designed settings, commits offsets manually, and reports lag 0 at the time of the live run.
- Event ordering holds per payment, which is the property the partition key is meant to give.
- What I would do differently: fix the topic auto-create problem before building anything else, because it caused most of the delays. Use a single consumer implementation from the start, instead of building the console consumer first. Add a dead-letter topic and a failure test. Size the 7-year retention and set up tiered storage before the production design.

---

## 10. Acceptance Criteria Status (project.md section 6)

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Design decisions documented, with rationale | Met | Section 1 and the three design files |
| 2 | Topic created with chosen configuration | Met | Section 2 (the payment topic is the `.v2` name; the old topic is an open issue) |
| 3 | Topic verified with describe output | Met | Section 2.3 and `scenarios/evidence/` |
| 4 | Producer running and sending mock events | Partly | Mock producer batches sent with the console tooling. No producer runs as a long-lived service |
| 5 | At least 2 complete payment journeys | Met | PAY-1001, SCN-LOW-001, SCN-MED-001, SCN-MED-002, LIVE2-COMP-001 |
| 6 | At least 3 consumer groups created | Met | Three groups: audit, fraud, notification. Each has committed offsets |
| 7 | Each consumer group produces meaningful output | Met | Section 4.2 sample output for each group |
| 8 | Event ordering verified | Met | Section 5.4 |
| 9 | Consumer lag monitored | Met | Section 5.1 and 5.2. Lag 0. Fraud and notification show no active member (issue 9) |
| 10 | Markdown file submitted | Met | This file, following the structure in section 7 of `project.md` |
| 11 | Trade-offs explained | Met | Section 6 |

### Submission checklist (project.md section 11)

- Design decisions documented: yes (Section 1)
- All commands included in code blocks: yes (Sections 2, 3, 4)
- Verification output: yes (Section 5, `scenarios/live_run/` and `scenarios/evidence/`)
- Event ordering verified: yes (Section 5.4)
- Lag monitoring: yes (Section 5.1 and 5.2)
- Trade-off explanations: yes (Section 6)
- Issues encountered: yes (Section 7)
- Conclusions: yes (Section 9)
- Markdown quality: this file

**Remaining work before a full sign-off:** the open gaps in Section 8.
