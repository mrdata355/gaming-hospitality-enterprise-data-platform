# Cloud Run services

`event_gateway` is a stateless ingestion boundary. It validates typed event contracts, writes a replayable landing object, then publishes the event to Pub/Sub with stable event/trace metadata.

Production hardening represented in the design:

- service account rather than embedded credentials
- authenticated ingress for batch/orchestration endpoints
- idempotency key bound to `event_id`
- immutable landing object creation using generation preconditions
- Pub/Sub dead-letter policies defined in Terraform
- structured trace/event attributes
- concurrency and max-instance limits to protect downstream systems
- separate budget alerts and log-based monitoring
