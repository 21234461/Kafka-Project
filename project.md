# Project Brief: Kafka Payment Events System Implementation

**Project Name:** Digital Payments Event Streaming Infrastructure  
**Team:** Digital Payments Engineering  


---

## **1. Business Context**

Your bank's digital payments team is implementing a real-time event streaming system to enable:

- **Real-time payments** (lag < 150ms)
- **Real-time fraud detection** (lag < 50ms)
- **Immediate customer notifications** (lag < 2s)
- **Regulatory compliance & audit** (7-year retention for fraud and payment events)
- **Target peak throughput of 1000Mb/s**
- **Assumption single instance producer throughput 100Mb/s for all producers**
- **Assumption single instance consumer throughput 10Mb/s for all consumers**


Currently, payment processing is synchronous and tightly coupled. The new Kafka-based architecture will decouple these concerns and enable independent scaling of fraud detection and notifications.

# 1 Payment producer
You are required to design a payment producer that will produce payment events into topic/s, which you will define, when users transact via digital payments. The payment entity goes through the following process: 
- payment initiated (publish event) 
- payment authorised/payment not authorised (publish event) 
- if payment is authorised: 
    - payment validated/payment invalidated (publish event)
    - if payment validated:
        - payment completed/not completed (publish event)

# 2 Fraud cosumer
You are required to design a fraud consumer that will consume payment initiated events and run a realtime fraud scoring

# 3 Fraud producer
You are required to design a fraud producer that will produce fraud events, into it's own topic/s when users transact via digital payments. This specific producer will just publish  
- fraud score low, high or medium events

# 4 Notification consumer
You are required to design a notification consumer that will consume payment failed and payment validated events and run a realtime notification sending

# 5 Notification producer
You are required to design a notification producer that will notification events, into it's own topic/s when users transact via digital payments. This specific producer will just publish  
- fraud payment approved or failed events

This project is a **proof of concept (POC)** that demonstrates the end-to-end system using mock producers and consumers.

---

## **2. Project Objectives**

By the end of this project, you will:

1.**Make informed design decisions** about topic configuration (partitions, replication, retention, compression, etc.)
2.**Justify your choices** — explain the trade-offs for each decision based on the requirements
3.**Design a payment event producer** that meets durability and idempotency requirements
4.**Design multiple consumers** representing different downstream use cases (fraud and notifications)
5.**Implement the system** using Kafka CLI tools and/or scripts
6.**Verify correctness** — show that ordering is preserved and lag is manageable
7.**Document everything** — submit a markdown file explaining your design decisions and demonstrating execution

---

## **3. Technical Requirements**

### **3.1 Kafka Cluster Setup**

You will work with a **local Kafka cluster** (provided).

### **3.2 Topic Design Task**

You must **design and justify** the following topic configuration decisions:

**Questions to Answer:**

1. **Topic Name:** What should the topic be called? (Consider naming conventions for your organization)

2. **Partitions:** How many partitions do you need?
   - Consider: throughput requirements, scalability, operational overhead
   - Think about: will different payment types need their own partitions, or can they coexist?
   - Justify your choice

3. **Replication Factor:** How many replicas should each partition have?
   - Consider: the requirement for zero data loss, operational cost, broker availability
   - What happens if a broker goes down? Can you afford to lose data?
   - Justify your choice

4. **Retention:** How long should payment events be retained?
   - Consider: regulatory compliance (7-year hold), storage cost, replay requirements
   - Should you have both time-based AND size-based retention? Why?
   - Justify your choice

5. **Cleanup Policy:** Should old messages be deleted, or should you use log compaction?
   - Consider: are payment events immutable, or can they be updated?
   - Is the topic an event stream, or a state store?
   - Justify your choice

6. **Compression:** Should messages be compressed?
   - Consider: message size, CPU cost, network bandwidth, storage savings
   - What compression algorithm makes sense for payment data?
   - Justify your choice

7. **Min In-Sync Replicas:** How many replicas must acknowledge a write before the producer considers it successful?
   - Consider: latency vs. durability trade-off
   - What's acceptable for payment initiation?
   - Justify your choice

8. **Message Schema:** What fields does a payment event need?
   - Consider: what information is required for fraud detection, notifications, reconciliation, and audit?

---

### **3.3 Producer Design Task**

**Producer Role:** Simulate a payment initiation system that publishes payment events to Kafka.

**Your Design Decisions:**

1. **Event Types:** What payment events should the producer publish?
   - What state transitions must occur (initiated → ??? → completed/failed)?
   - Which events are critical to capture?
   - Design the event schema (which fields, what data types?)

2. **Partition Key Strategy:** How should the producer decide which partition to write to?
   - Consider: what guarantees do downstream consumers need (ordering, isolation)?
   - What key would make sense for payment data?
   - Justify your choice (remember: goal is to enable fraud detection + reconciliation)

3. **Durability:** How important is it that each payment event reaches Kafka without loss?
   - Consider: the business cost of losing a payment event
   - Should the producer wait for all replicas to acknowledge, or is speed more important?
   - What's your durability setting (`acks`)?

4. **Idempotency:** What if the producer network hiccup and retries a message?
   - Should duplicate payments be possible?
   - How would you prevent duplicates?
   - What configuration or mechanism is needed?

5. **Serialization Format:** How should events be encoded?
   - JSON (human-readable, larger)
   - Avro (schema-driven, compact)
   - Protobuf (binary, fast)
   - What are the trade-offs for payment data?

6. **Error Handling:** What if Kafka is down and the producer can't send?
   - Should you retry? How many times? How long?
   - Should you fail fast or wait patiently?
   - What's the acceptable window of time a payment can be "stuck" in the producer?

7. **Production Scale:** How many payment events should you generate for testing?
   - What realistic payment patterns would you simulate?
   - Should you include failed payments? Rejected payments?

---

### **3.4 Consumer Design Task**

You must create **at least 3 independent consumer groups**, each representing a different downstream system. Based on the business requirements, what consumers do you need?

**Design Questions for Each Consumer:**

1. **Consumer Purpose & SLA:** 
   - What is this consumer responsible for (fraud detection, notifications, reconciliation, audit)?
   - What's the acceptable lag (near real-time, < 1s, < 1 hour)?
   - Why does that lag matter for this use case?

2. **Consumer Group Name:** 
   - What should the consumer group be called?
   - Follow naming conventions (hyphen-separated, descriptive)

3. **Event Filtering:**
   - Does this consumer need all events, or only specific event types?
   - How would you filter?

4. **Processing Logic:**
   - What should the consumer do with each event?
   - What output/action should it produce?
   - How will you detect if processing fails?

5. **Offset Management:**
   - Should offsets be committed automatically or manually?
   - Why does it matter for this consumer?

6. **Ordering & Isolation:**
   - Does this consumer care about exactly-once delivery?
   - What if the same event is processed twice?
   - Can the consumer afford to miss events?

7. **Scalability:**
   - Should this consumer be horizontally scalable (multiple instances)?
   - How many partitions do you expect it to read from?


---

## **4. Project Deliverables**

Submit a markdown file (`payment_events_setup.md`) containing:

### **4.1 System Design Document**

Document your **design decisions** for:

1. **Topic Design**
   - Topic name and justification
   - Number of partitions and why
   - Replication factor and why
   - Retention policy (time/size) and why
   - Cleanup policy and why
   - Compression settings and why
   - Min in-sync replicas and why
   - Message schema with field descriptions

2. **Producer Design**
   - Event types you're publishing
   - Partition key strategy and justification
   - Durability (`acks` setting) and justification
   - Idempotency approach and why it matters
   - Serialization format and why
   - Error handling & retry strategy
   - How many mock events you'll produce

3. **Consumer Design**
   - List of consumer groups and their purposes
   - For each consumer:
     - Consumer group name
     - Lag SLA and why
     - What events it processes
     - Processing logic
     - Error handling approach
     - Scalability considerations

### **4.2 Topic Creation**

Document:
- Full `kafka-topics.sh --create` command with all your chosen configurations
- Output from `kafka-topics.sh --describe --topic <your-topic-name>`

### **4.3 Producer Setup**

Document:
- Producer creation commands (console producer OR custom script)
- How many mock payment events you produced
- Example events (at least 2 different event types)
- Evidence of successful production (offsets, partition assignments)
- Any decisions you made during testing

### **4.4 Consumer Setup**

Document:
- Consumer creation commands for each consumer group
- Consumer group configurations
- Sample output showing processing
- Any filtering or transformations applied
- Error handling in action (if applicable)

### **4.5 Verification & Monitoring**

Document:
- Consumer group status commands and results
- Consumer lag for each group
- Topic status and configuration
- Event ordering verification (show that related events stay together)
- Any issues encountered and how you resolved them

---

## **5. Implementation Workflow**

### **Step 1: Kafka Cluster Setup**

Ensure your Kafka cluster is running with all required components (Zookeeper, Brokers, optional Schema Registry).

**Your task:** Document how to start the cluster components in your environment.

### **Step 2: Design Your Topic**

Based on the requirements in Section 3.2, make decisions on:
- Topic name
- Number of partitions (consider: throughput, scalability, consumer parallelism)
- Replication factor (consider: data loss risk, operational cost)
- Retention (consider: regulatory hold, storage budget, replay capability)
- Cleanup policy (consider: are events immutable or does state change?)
- Compression (consider: message size, CPU cost)
- Min in-sync replicas (consider: latency vs. durability)

**Your task:** Write the `kafka-topics.sh --create` command based on your decisions.

### **Step 3: Create the Topic**

Execute your topic creation command.

**Your task:** Verify successful creation using `kafka-topics.sh --describe`. Document the output.

### **Step 4: Design Your Producer**

Based on the requirements in Section 3.3, make decisions on:
- What payment events to publish (what state transitions?)
- Partition key strategy (how to ensure ordering?)
- Durability level (`acks` setting)
- Idempotency approach (prevent duplicates?)
- Serialization format (JSON, Avro, Protobuf?)
- Error handling (retry, fail fast, DLQ?)
- How many mock events to produce

**Your task:** Decide whether to use `kafka-console-producer.sh` or write a custom producer script.

### **Step 5: Produce Mock Events**

Create realistic payment event flows:
- Multiple payment journeys (initiated → authorized → completed)
- Some payments that fail (to test error handling)
- Variety of payment types/channels

**Your task:** Document commands and show evidence of successful production.

### **Step 6: Design Your Consumers**

Based on the requirements in Section 3.4 and your business needs, design consumer groups:
- What consumer groups do you need?
- What's each one's purpose and lag SLA?
- How will each one process events?
- What filtering (if any)?

**Your task:** Document decisions for each consumer group.

### **Step 7: Create Consumers**

For each consumer group, create a consumer instance.

**Your task:** Show consumer group creation commands and verify they're running.

### **Step 8: Verify Ordering & Processing**

Test that your system works correctly:
- Do related events stay together (same partition, sequential offsets)?
- Are all consumers receiving events?
- Is lag near zero?

**Your task:** Document verification steps and results.

### **Step 9: Document Everything**

Document all design decisions and commands in your markdown file.

---

## **6. Acceptance Criteria**

Your project is complete when:

- **Design decisions documented** — rationale provided for all topic, producer, and consumer choices
- **Topic created** with your chosen configuration
- **Topic verified** — describe output shows your configuration is in place
- **Producer running** and sending mock payment events
- **Multiple payment flows produced** — show at least 2 complete journeys (initiation through completion/failure)
- **Consumer groups created** — at least 3 independent consumers representing different use cases
- **Consumers processing** — each consumer group produces meaningful output based on its purpose
- **Event ordering verified** — related payment events stay together in the same partition
- **Consumer lag monitored** — lag is low/zero, indicating consumers are caught up
- **Markdown file submitted** with all design decisions, commands, output, and explanations
- **Trade-offs explained** — justify why you chose what you chose (not just "the default")

---

## **7. Your Markdown File Structure**

Your `payment_events_setup.md` should have sections for:

1. **Design Decisions** — Explain your choices for topic, producer, and consumer configuration
2. **Topic Creation** — Your `kafka-topics.sh` command and verification output
3. **Producer Setup** — Producer commands and evidence of events sent
4. **Consumer Groups** — For each consumer, document its command and output
5. **Verification** — Show that your system works (lag, ordering, processing)
6. **Trade-offs & Justifications** — Why did you choose what you chose?
7. **Issues Encountered** — What went wrong? How did you fix it?

**Note:** Focus on *explaining your thinking*, not just showing commands. The goal is to demonstrate that you made informed decisions based on the requirements and trade-offs.

---

## **8. Common Issues & Debugging**

When implementing your system, you may encounter:

- **Topic creation fails** — Is the Kafka broker running? Is the bootstrap server address correct?
- **No messages in consumers** — Did you produce events first? Did consumers start after production?
- **Consumers blocked** — Is the consumer processing fast enough? Check consumer logs for errors.
- **High lag** — Is the consumer group behind? How many events are unprocessed?
- **Events appear out of order** — Did you use a partition key? Is it consistent?
- **Memory issues** — Are you fetching too many messages at once? Adjust fetch batch sizes.

**Document any issues you encountered and how you resolved them.**

---



## **10. Resources**

- [Kafka Documentation: Topics](https://kafka.apache.org/documentation/#topicconfigs)
- [Kafka CLI Tools Reference](https://kafka.apache.org/documentation/#tools)
- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumerconfigs)
- [Schema Evolution & Compatibility](https://docs.confluent.io/kafka/schema-registry/schema-evolution/index.html)

---

## **11. Submission Checklist**

Before submitting `payment_events_setup.md`, ensure:

- **Design decisions documented** — Topic, producer, and consumer choices with rationale
- **All commands included** — Topic creation, producer, consumer commands in code blocks
- **Verification output** — Screenshots or console output showing successful execution
- **Event ordering verified** — Show that related events stay together
- **Lag monitoring** — Consumer group status for all groups
- **Trade-off explanations** — Why did you choose those configurations?
- **Issues encountered** — What didn't work initially? How did you debug/fix it?
- **Conclusions** — What did you learn? What would you do differently?
- **Markdown quality** — File is readable; proper formatting and code blocks

---
 