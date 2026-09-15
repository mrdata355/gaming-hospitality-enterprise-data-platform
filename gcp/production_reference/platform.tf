terraform {
  required_version = ">= 1.7.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

variable "project_id" {
  type        = string
  description = "Synthetic portfolio GCP project id."
}

variable "region" {
  type        = string
  default     = "us-west1"
  description = "Generated portfolio region; not a statement about private infrastructure."
}

variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "qa", "stage", "prod"], var.environment)
    error_message = "environment must be dev, qa, stage, or prod"
  }
}

variable "labels" {
  type = map(string)
  default = {
    platform            = "gaming-hospitality-enterprise"
    data_classification = "synthetic-portfolio"
    owner               = "data-platform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  prefix = "vh-portfolio-${var.environment}"
  zones = toset([
    "landing",
    "raw",
    "clean",
    "merge",
    "curated",
    "quarantine",
    "replay",
    "checkpoints"
  ])
  topics = toset([
    "guest.mobile-events.v1",
    "hotel.reservation-events.v1",
    "hotel.checkin-events.v1",
    "gaming.slot-play-events.v1",
    "gaming.table-play-events.v1",
    "rewards.earn-events.v1",
    "rewards.redemption-events.v1",
    "marketing.offer-events.v1",
    "property.transaction-events.v1",
    "ops.pipeline-events.v1"
  ])
  datasets = toset([
    "raw",
    "clean",
    "merge",
    "curated",
    "audit"
  ])
}

resource "google_storage_bucket" "lake" {
  for_each                    = local.zones
  name                        = "${local.prefix}-${each.key}-${var.region}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = var.environment != "prod"
  labels                      = merge(var.labels, { zone = each.key })

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = each.key == "raw" ? 30 : 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = each.key == "quarantine" ? 365 : 2555
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_pubsub_topic" "domain" {
  for_each = local.topics
  name     = "${local.prefix}-${replace(each.key, ".", "-")}"
  labels   = merge(var.labels, { topic = replace(each.key, ".", "_") })

  message_retention_duration = "86600s"
}

resource "google_pubsub_topic" "dlq" {
  for_each = local.topics
  name     = "${local.prefix}-${replace(each.key, ".", "-")}-dlq"
  labels   = merge(var.labels, { purpose = "dead_letter" })
}

resource "google_pubsub_subscription" "domain" {
  for_each = local.topics
  name     = "${local.prefix}-${replace(each.key, ".", "-")}-consumer-v1"
  topic    = google_pubsub_topic.domain[each.key].id

  ack_deadline_seconds       = 30
  message_retention_duration = "604800s"
  retain_acked_messages      = false
  enable_message_ordering    = true

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq[each.key].id
    max_delivery_attempts = 10
  }

  expiration_policy {
    ttl = ""
  }
}

resource "google_service_account" "stream_consumer" {
  account_id   = "vh-${var.environment}-stream-consumer"
  display_name = "Synthetic portfolio stream consumer"
}

resource "google_service_account" "batch_loader" {
  account_id   = "vh-${var.environment}-batch-loader"
  display_name = "Synthetic portfolio batch loader"
}

resource "google_service_account" "curated_publisher" {
  account_id   = "vh-${var.environment}-curated-publisher"
  display_name = "Synthetic portfolio curated publisher"
}

resource "google_bigquery_dataset" "layer" {
  for_each                  = local.datasets
  dataset_id                = "vh_${each.key}_${var.environment}"
  location                  = var.region
  delete_contents_on_destroy = var.environment != "prod"
  labels                    = merge(var.labels, { layer = each.key })
}

resource "google_bigquery_table" "reservation_current" {
  dataset_id = google_bigquery_dataset.layer["merge"].dataset_id
  table_id   = "hotel_reservation_current"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "arrival_date"
  }

  clustering = ["property_code", "reservation_status"]

  schema = jsonencode([
    { name = "reservation_id", type = "STRING", mode = "REQUIRED" },
    { name = "guest_id", type = "STRING", mode = "NULLABLE" },
    { name = "property_code", type = "STRING", mode = "REQUIRED" },
    { name = "arrival_date", type = "DATE", mode = "REQUIRED" },
    { name = "departure_date", type = "DATE", mode = "REQUIRED" },
    { name = "reservation_status", type = "STRING", mode = "REQUIRED" },
    { name = "room_type_code", type = "STRING", mode = "NULLABLE" },
    { name = "rooms", type = "INTEGER", mode = "REQUIRED" },
    { name = "revenue_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "source_version", type = "INTEGER", mode = "REQUIRED" },
    { name = "source_updated_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "updated_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])
}

resource "google_bigquery_table" "gaming_event" {
  dataset_id = google_bigquery_dataset.layer["clean"].dataset_id
  table_id   = "gaming_slot_play"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "event_time"
  }

  clustering = ["property_code", "machine_id"]

  schema = jsonencode([
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "play_id", type = "STRING", mode = "REQUIRED" },
    { name = "machine_id", type = "STRING", mode = "REQUIRED" },
    { name = "property_code", type = "STRING", mode = "REQUIRED" },
    { name = "event_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_type", type = "STRING", mode = "REQUIRED" },
    { name = "coin_in", type = "NUMERIC", mode = "NULLABLE" },
    { name = "payout", type = "NUMERIC", mode = "NULLABLE" },
    { name = "jackpot_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "schema_version", type = "STRING", mode = "REQUIRED" },
    { name = "source_partition", type = "INTEGER", mode = "NULLABLE" },
    { name = "source_offset", type = "INTEGER", mode = "NULLABLE" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])
}

resource "google_cloud_run_v2_service" "event_gateway" {
  name     = "${local.prefix}-event-gateway"
  location = var.region

  template {
    service_account = google_service_account.stream_consumer.email

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello"

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "RAW_BUCKET"
        value = google_storage_bucket.lake["raw"].name
      }
      env {
        name  = "QUARANTINE_BUCKET"
        value = google_storage_bucket.lake["quarantine"].name
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
  }
}

resource "google_cloud_run_v2_service" "stream_consumer" {
  name     = "${local.prefix}-stream-consumer"
  location = var.region

  template {
    service_account = google_service_account.stream_consumer.email

    scaling {
      min_instance_count = 0
      max_instance_count = 25
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello"

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "CLEAN_BUCKET"
        value = google_storage_bucket.lake["clean"].name
      }
      env {
        name  = "MERGE_BUCKET"
        value = google_storage_bucket.lake["merge"].name
      }
      env {
        name  = "CHECKPOINT_BUCKET"
        value = google_storage_bucket.lake["checkpoints"].name
      }

      resources {
        limits = {
          cpu    = "2"
          memory = "1Gi"
        }
      }
    }
  }
}

resource "google_project_iam_member" "consumer_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_project_iam_member" "publisher_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.batch_loader.email}"
}

resource "google_storage_bucket_iam_member" "consumer_raw" {
  bucket = google_storage_bucket.lake["raw"].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_storage_bucket_iam_member" "consumer_clean" {
  bucket = google_storage_bucket.lake["clean"].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_storage_bucket_iam_member" "consumer_merge" {
  bucket = google_storage_bucket.lake["merge"].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_storage_bucket_iam_member" "consumer_quarantine" {
  bucket = google_storage_bucket.lake["quarantine"].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_bigquery_dataset_iam_member" "consumer_clean" {
  dataset_id = google_bigquery_dataset.layer["clean"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_bigquery_dataset_iam_member" "consumer_merge" {
  dataset_id = google_bigquery_dataset.layer["merge"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.stream_consumer.email}"
}

resource "google_bigquery_dataset_iam_member" "curated_editor" {
  dataset_id = google_bigquery_dataset.layer["curated"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.curated_publisher.email}"
}

output "lake_buckets" {
  value = { for key, bucket in google_storage_bucket.lake : key => bucket.name }
}

output "topic_names" {
  value = { for key, topic in google_pubsub_topic.domain : key => topic.name }
}

output "subscription_names" {
  value = { for key, sub in google_pubsub_subscription.domain : key => sub.name }
}

output "dataset_ids" {
  value = { for key, dataset in google_bigquery_dataset.layer : key => dataset.dataset_id }
}

output "gateway_uri" {
  value = google_cloud_run_v2_service.event_gateway.uri
}

output "consumer_uri" {
  value = google_cloud_run_v2_service.stream_consumer.uri
}
