variable "bucket_name" {
  type = string
}

variable "push_endpoint" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "filter" {
  type    = string
  default = ""
}

# https://cloud.google.com/pubsub/docs/push#configure_for_push_authentication
# The audience identifies the recipients that the JWT is intended for. If unset, this will default
# to the value of push_endpoint. For Cloud Run, that is sufficient. For App Engine, this should be
# set to the IAP Client ID.
variable "oidc_audience" {
  type    = string
  default = ""
}

variable "ack_deadline_seconds" {
  type    = number
  default = 10
}

# Optional suffix to differentiate storage notifications for the same bucket.
# In the future it may make more sense to allow for just one topic with multiple subscriptions, but
# this was easier to implement.
variable "suffix" {
  type    = string
  default = ""
}

locals {
  notification_name = "storage-notification-${var.bucket_name}${var.suffix != "" ? "-${var.suffix}" : ""}"
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
  name = local.notification_name
}

resource "google_pubsub_subscription" "subscription" {
  name   = local.notification_name
  topic  = google_pubsub_topic.topic.name
  filter = var.filter

  push_config {
    push_endpoint = var.push_endpoint
    oidc_token {
      service_account_email = var.service_account_email
      audience              = var.oidc_audience
    }
  }

  ack_deadline_seconds = var.ack_deadline_seconds

}
moved {
  from = google_pubsub_subscription.example
  to   = google_pubsub_subscription.subscription
}
