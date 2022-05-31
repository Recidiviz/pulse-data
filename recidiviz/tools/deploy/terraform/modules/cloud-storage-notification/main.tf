variable "bucket_name" {
  type = string
}

variable "push_endpoint" {
  type = string
}

variable "service_account_email" {
  type = string
}

resource "google_storage_notification" "notification" {
  bucket         = var.bucket_name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.topic.id
  event_types    = ["OBJECT_FINALIZE"]
  depends_on     = [google_pubsub_topic_iam_binding.binding]
}

// Enable notifications by giving the correct IAM permission to the unique service account.

data "google_storage_project_service_account" "gcs_account" {
}

resource "google_pubsub_topic_iam_binding" "binding" {
  topic   = google_pubsub_topic.topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}

resource "google_pubsub_topic" "topic" {
  name = "storage-notification-${var.bucket_name}"
}

resource "google_pubsub_subscription" "subscription" {
  name  = "storage-notification-${var.bucket_name}"
  topic = google_pubsub_topic.topic.name

  push_config {
    push_endpoint = var.push_endpoint
    oidc_token {
      service_account_email = var.service_account_email
    }
  }
}
moved {
  from = google_pubsub_subscription.example
  to   = google_pubsub_subscription.subscription
}
