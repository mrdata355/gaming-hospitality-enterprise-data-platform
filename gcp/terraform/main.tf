terraform {
  required_version = ">= 1.8.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  prefix = "gh-${var.environment}"
  topics = toset([
    "gaming.slot-play-events.v1",
    "gaming.table-play-events.v1",
    "hotel.reservation-events.v1",
    "hotel.checkin-events.v1",
    "guest.mobile-events.v1",
    "rewards.earn-events.v1",
    "rewards.redemption-events.v1",
    "marketing.offer-events.v1",
    "property.transaction-events.v1",
    "ops.pipeline-events.v1",
  ])
  buckets = toset(["landing", "bronze", "silver", "gold", "quarantine"])
}

resource "google_project_service" "services" {
  for_each = toset([
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "artifactregistry.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "medallion" {
  for_each = local.buckets

  name                        = "${local.prefix}-${each.key}"
  location                    = "US"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = each.key == "landing" ? 30 : 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  depends_on = [google_project_service.services]
}

resource "google_pubsub_topic" "event" {
  for_each = local.topics

  name                       = each.value
  message_retention_duration = "604800s"

  depends_on = [google_project_service.services]
}

resource "google_pubsub_topic" "dead_letter" {
  name = "ops.dead-letter.v1"
}

resource "google_service_account" "event_gateway" {
  account_id   = "${local.prefix}-event-gateway"
  display_name = "Gaming Hospitality Event Gateway"
}

resource "google_service_account" "event_consumer" {
  account_id   = "${local.prefix}-event-consumer"
  display_name = "Gaming Hospitality PubSub Consumer"
}

resource "google_service_account" "pubsub_invoker" {
  account_id   = "${local.prefix}-pubsub-invoker"
  display_name = "PubSub Cloud Run Invoker"
}

resource "google_cloud_run_v2_service" "event_gateway" {
  name                = "${local.prefix}-event-gateway"
  location            = var.region
  deletion_protection = true

  template {
    service_account                  = google_service_account.event_gateway.email
    max_instance_request_concurrency = 40

    scaling {
      min_instance_count = 0
      max_instance_count = 5
    }

    containers {
      image = var.cloud_run_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }

      env {
        name  = "RESORT_GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "RESORT_ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "RESORT_GCS_LANDING_BUCKET"
        value = google_storage_bucket.medallion["landing"].name
      }
    }
  }

  depends_on = [google_project_service.services]
}

resource "google_cloud_run_v2_service" "event_consumer" {
  name                = "${local.prefix}-event-consumer"
  location            = var.region
  deletion_protection = true

  template {
    service_account                  = google_service_account.event_consumer.email
    max_instance_request_concurrency = 80

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }

    containers {
      image   = var.cloud_run_image
      command = ["uvicorn"]
      args = [
        "resort_platform.services.pubsub_consumer:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8080",
      ]

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }

      env {
        name  = "RESORT_GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "RESORT_ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "RESORT_GCS_BRONZE_BUCKET"
        value = google_storage_bucket.medallion["bronze"].name
      }
      env {
        name  = "RESORT_GCS_QUARANTINE_BUCKET"
        value = google_storage_bucket.medallion["quarantine"].name
      }
    }
  }

  depends_on = [google_project_service.services]
}

resource "google_cloud_run_v2_service_iam_member" "consumer_invoker" {
  project  = var.project_id
  location = google_cloud_run_v2_service.event_consumer.location
  name     = google_cloud_run_v2_service.event_consumer.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}

resource "google_project_iam_member" "pubsub_token_creator" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription" "event" {
  for_each = google_pubsub_topic.event

  name                       = "${replace(each.key, ".", "-")}-cloudrun"
  topic                      = each.value.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 10
  }

  push_config {
    push_endpoint = "${google_cloud_run_v2_service.event_consumer.uri}/v1/pubsub/push"
    oidc_token {
      service_account_email = google_service_account.pubsub_invoker.email
      audience              = google_cloud_run_v2_service.event_consumer.uri
    }
  }
}

resource "google_project_iam_member" "gateway_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.event_gateway.email}"
}

resource "google_storage_bucket_iam_member" "gateway_landing" {
  bucket = google_storage_bucket.medallion["landing"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.event_gateway.email}"
}

resource "google_storage_bucket_iam_member" "consumer_bronze" {
  bucket = google_storage_bucket.medallion["bronze"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.event_consumer.email}"
}

resource "google_storage_bucket_iam_member" "consumer_quarantine" {
  bucket = google_storage_bucket.medallion["quarantine"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.event_consumer.email}"
}
