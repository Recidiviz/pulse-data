data "sops_file" "config" {
  source_file = "./config.production.enc.yaml"
}

locals {
  sops_data = sensitive(yamldecode(data.sops_file.config.raw))
  emails = local.sops_data["emails"]
  service_keys = local.sops_data["service_keys"]
}

resource "google_monitoring_notification_channel" "polaris_general_infrastructure" {
  display_name = "[Polaris] General Infrastructure"
  enabled      = "true"
  force_delete = "false"
  project      = var.project_id
  type         = "pagerduty"

  labels = {
    "service_key" = local.service_keys["polaris"]
  }
}

resource "google_monitoring_notification_channel" "alerts" {
  display_name = local.emails["alerts"]
  enabled      = "true"
  force_delete = "false"

  labels = {
    email_address = local.emails["alerts"]
  }

  project = var.project_id
  type    = "email"
}

resource "google_monitoring_notification_channel" "pagerduty_cloud_billing" {
  display_name = "PagerDuty (Cloud Billing)"
  enabled      = "true"
  force_delete = "false"
  project      = var.project_id
  type         = "pagerduty"

  labels = {
    "service_key" = local.service_keys["cloud_billing"]
  }
}

resource "google_monitoring_notification_channel" "pagerduty_cloud_billing_2" {
  display_name = "PagerDuty (Cloud Billing)"
  enabled      = "true"
  force_delete = "false"

  labels = {
    email_address = local.emails["cloud_billing"]
  }

  project = var.project_id
  type    = "email"
}

resource "google_monitoring_notification_channel" "stackdriver" {
  display_name = "Stackdriver"
  enabled      = "true"
  force_delete = "false"
  project      = var.project_id
  type         = "pagerduty"
  labels = {
    "service_key" = local.service_keys["stackdriver"]
  }
}

resource "google_monitoring_notification_channel" "ie_on_call_general_alerts" {
  display_name = "IE On-Call: General Alerts"
  enabled      = "true"
  force_delete = "false"
  project      = var.project_id
  type         = "pagerduty"
  labels = {
    "service_key" = local.service_keys["ie_on_call"]
  }
}

resource "google_monitoring_notification_channel" "security" {
  display_name = local.emails["security"]
  enabled      = "true"
  force_delete = "false"

  labels = {
    email_address = local.emails["security"]
  }

  project = var.project_id
  type    = "email"
}

resource "google_monitoring_notification_channel" "pagerduty_cloud_billing_backup" {
  display_name = "PagerDuty (Cloud Billing backup)"
  enabled      = "true"
  force_delete = "false"

  labels = {
    email_address = local.emails["cloud_billing_backup"]
  }

  project = var.project_id
  type    = "email"
}


data "google_monitoring_notification_channel" "pagerduty_alert_forwarder_service" {
  project = var.project_id
  display_name = "PagerDuty Alert Forwarder"
  type = "pubsub"
}
