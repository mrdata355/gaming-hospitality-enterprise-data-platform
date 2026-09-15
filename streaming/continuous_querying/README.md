# Continuous Streaming / Data Streaming / Continuous Querying

This folder was added after interview feedback emphasized **SQL, GCP, Azure, SSIS, continuous streaming, data streaming, and continuous querying**.

All resources are synthetic, project-owned portfolio assets. They model production engineering patterns without claiming knowledge of private Venetian systems.

## Interview definition

A **continuous query** is a long-running query over an unbounded event stream. Unlike a batch query that reads a bounded dataset and exits, a continuous query remains active, tracks source progress, maintains state where required, and emits new or updated results as events arrive.

```text
SOURCE
  -> SCHEMA
  -> EVENT TIME
  -> WATERMARK
  -> VALIDATE / QUARANTINE
  -> DEDUPE
  -> STATE / WINDOW
  -> CHECKPOINT
  -> IDEMPOTENT SINK
  -> LAG / THROUGHPUT / STATE METRICS
  -> REPLAY
```

## Important distinction: continuous streaming vs Spark Continuous Processing

Use precise language in the interview:

- **Continuous streaming** is the broad architecture: a long-running pipeline processing an unbounded source.
- **Spark Structured Streaming** normally uses micro-batches for many stateful production workloads.
- Spark also has a specific **Continuous Processing** trigger with a narrower operator surface.
- Stateful watermarking, deduplication, complex windows, stream-stream joins, and `foreachBatch` transactional merge patterns are reasons to use Structured Streaming micro-batch rather than forcing the special Continuous Processing trigger.

So if asked whether you use continuous streaming, the answer can be yes even when the Spark execution engine is micro-batch.

## Repository mapping

```text
streaming/continuous_querying/
    continuous_streaming_reference.py
        event-time handling
        watermarks
        dedupe
        checkpoints
        state
        idempotent sink patterns
        query progress / health
        replay and graceful shutdown

gcp/dataflow/
    continuous_streaming_pipeline.py
        Pub/Sub
        Apache Beam / Dataflow
        fixed, sliding and session windows
        late data
        BigQuery / lake outputs
        quarantine

azure/streaming/
    stream_analytics_continuous_queries.sql
        Event Hubs / Stream Analytics model
        TIMESTAMP BY
        tumbling windows
        hopping windows
        session windows
        temporal correlation

sqlserver/continuous_querying/
    operational_streaming_support.sql
        transactional outbox
        claim / lease / ack
        idempotent consumer ledger
        current-state MERGE
        streaming audit and reconciliation

ssis/continuous_handoff/
    CDC_To_EventBus_Continuous_Handoff.dtsx.xml
        bounded CDC extraction
        source watermarks
        stable event IDs
        transactional outbox
        event-publisher handoff
        reconciliation
```

## GCP model

```text
DB2 / SQL Server / mobile / application events
                    |
                    v
                 Pub/Sub
                    |
                    v
              Dataflow stream
                    |
       +------------+-------------+
       |            |             |
       v            v             v
     raw lake     BigQuery      quarantine
       |
       v
 clean -> merge -> curated
```

## Azure model

```text
SQL Server / apps
       |
       v
   Event Hubs
       |
       v
Azure Stream Analytics
       |
   +---+------------------+
   |                      |
   v                      v
operational SQL         ADLS
                          |
                          v
                    ADF downstream
                    orchestration
```

ADF remains an orchestration and migration tool in this model. It is not presented as the stateful continuous stream-processing engine.

## SSIS boundary

SSIS is used for legacy integration, CDC extraction, bounded watermarks, staging, package logging, reconciliation, and handoff to an event publisher. It should not be described as a replacement for Pub/Sub + Dataflow, Event Hubs + Stream Analytics, or Spark Structured Streaming when the requirement is a continuously running stateful streaming engine.

## Core concepts

### Event time vs processing time

**Event time** is when the business event occurred. **Processing time** is when the streaming system handled it. Business windows normally use event time when ordering matters.

### Watermark

A watermark is an event-time progress boundary used to control how long the engine retains state for late events. It is not simply the maximum timestamp observed.

### Late data

Events inside the accepted lateness horizon can update state. Events beyond it need an explicit path: quarantine, correction, replay, or business-specific reconciliation.

### Deduplication

Use a stable `event_id` and, when required, a business key plus source version. Do not deduplicate solely by processing timestamp.

### Checkpointing

A checkpoint stores source progress and state metadata so a long-running query can restart without beginning from zero.

### Exactly-once discussion

Do not say a framework alone guarantees exactly-once behavior. Use this sequence:

```text
REPLAYABLE SOURCE
-> CHECKPOINT / PROGRESS
-> DETERMINISTIC TRANSFORM
-> STABLE EVENT / BUSINESS KEY
-> IDEMPOTENT OR TRANSACTIONAL SINK
```

End-to-end outcome semantics depend on the entire source-to-sink path.

### Backpressure / lag

If input rate exceeds processing rate, backlog grows. Monitor:

- input rows per second
- processed rows per second
- source lag
- batch / trigger duration
- watermark movement
- state rows and state memory
- worker or executor saturation
- sink commit latency

### Windows

- **Tumbling:** fixed, non-overlapping windows.
- **Hopping / sliding:** overlapping windows that advance by a smaller interval.
- **Session:** windows defined by inactivity gaps.

## 30-second answer

> I design streaming around event time and replayability. I define the event contract and stable event ID first, process with watermarks and bounded state, quarantine invalid records, checkpoint progress, and write through an idempotent or transactional sink. I monitor input rate versus processing rate, lag, trigger duration, watermark movement and state size. In Spark I normally use Structured Streaming micro-batch for stateful workloads; if someone says Continuous Processing, I treat that as a specific execution mode with a more limited operator surface, not as a synonym for every continuous stream.

## Failure scenario

If a continuous query falls behind:

1. Confirm the source is healthy and determine blast radius.
2. Compare input rate to processing rate.
3. Inspect lag, task skew, sink latency, worker saturation, state size and watermark movement.
4. Preserve the source and checkpoint before changing the runtime.
5. Fix or mitigate the actual bottleneck.
6. Restart from the checkpoint.
7. Replay only the affected interval when necessary.
8. Reconcile source = applied + duplicate + quarantine + intentionally dropped/stale records.
9. Record root cause and add a preventive control.

## Follow-up questions to practice

- Event time vs processing time?
- What does a watermark actually do?
- How do you handle late arrivals?
- How do you prevent duplicates after restart?
- What happens if the sink commits but progress does not?
- How do you replay one time range?
- What metrics identify streaming lag?
- Dataflow vs Spark Structured Streaming?
- Pub/Sub vs polling a source database?
- How does SSIS hand off into an event-driven platform?
- Tumbling vs hopping vs session windows?
- How do you keep state from growing forever?
- What makes a sink idempotent?
- How do schema changes avoid breaking consumers?
