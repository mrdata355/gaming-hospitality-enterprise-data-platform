output "medallion_buckets" { value = { for k, v in google_storage_bucket.medallion : k => v.name } }
output "pubsub_topics" { value = { for k, v in google_pubsub_topic.event : k => v.id } }
output "event_gateway_uri" { value = google_cloud_run_v2_service.event_gateway.uri }
