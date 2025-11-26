variable "project_id" {
  type = string
}

resource "google_monitoring_alert_policy" "vpc_net_route_changes_cis" {
  alert_strategy {
    auto_close = "604800s"
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = "resource.type=\"global\" AND metric.type=\"logging.googleapis.com/user/vpc_net_route_changes_cis\""
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "vpc_net_route_changes_cis"
  }

  display_name          = "vpc_net_route_changes_cis"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    google_monitoring_notification_channel.security.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cis_audit_configuration_changes" {
  alert_strategy {
    auto_close = "604800s"
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "gce_instance" AND metric.type="logging.googleapis.com/user/audit_config_changes_cis"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "CIS_Audit_Configuration_ChangeS"
  }

  display_name          = "CIS_Audit_Configuration_Changes"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    google_monitoring_notification_channel.security.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_sql_high_memory_utilization" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      comparison      = "COMPARISON_GT"
      duration        = "600s"
      filter          = <<-EOT
        resource.type = "cloudsql_database" AND metric.type = "cloudsql.googleapis.com/database/memory/utilization"
      EOT
      threshold_value = "0.7"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud SQL: High Memory Utilization"
  }

  display_name = "Cloud SQL: High Memory Utilization"

  documentation {
    content   = "This alert means that memory utilization for Cloud SQL has reached an undesirably high percentage for an undesirably long duration of time. Visit the Cloud SQL console to determine if we need to expand the memory allotment for the instance in question: https://console.cloud.google.com/sql/instances?project=recidiviz-123\n\nCheck out the Database wiki page on pulse-data and/or pulse-data for information on database administration: https://github.com/Recidiviz/pulse-data/wiki\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_run_job_failure" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "900s"
        cross_series_reducer = "REDUCE_COUNT"
        group_by_fields      = ["resource.label.project_id", "resource.label.job_name"]
        per_series_aligner   = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "cloud_run_job" AND metric.type = "run.googleapis.com/job/completed_task_attempt_count" AND metric.labels.result = "failed"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud Run Job - Failed"
  }

  display_name = "Cloud Run Job Failure"

  documentation {
    content   = "View recent runs at:\nhttps://console.cloud.google.com/run/jobs?project=recidiviz-dashboard-production"
    mime_type = "text/markdown"
    subject   = "Cloud Run Job Failed"
  }

  enabled               = "true"
  notification_channels = [
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
    google_monitoring_notification_channel.alerts.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "uptime_check_url_down" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_MEAN"
        group_by_fields      = ["metric.label.checked_resource_id"]
        per_series_aligner   = "ALIGN_FRACTION_TRUE"
      }

      comparison              = "COMPARISON_LT"
      duration                = "900s"
      evaluation_missing_data = "EVALUATION_MISSING_DATA_ACTIVE"
      filter                  = <<-EOT
        resource.type = "uptime_url" AND metric.type = "monitoring.googleapis.com/uptime_check/check_passed" AND (metric.labels.checked_resource_id != "recidiviz.org" AND metric.labels.checker_location = starts_with("us"))
      EOT
      threshold_value         = "0.95"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Uptime Check URL - Successful checks fell below 95%"
  }

  display_name          = "Uptime Check URL - DOWN"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.polaris_general_infrastructure.id,
    google_monitoring_notification_channel.alerts.id
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_sql_instance_update_detection" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "600s"
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner   = "ALIGN_DELTA"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = "metric.type=\"logging.googleapis.com/user/cloud_sql_instance_update\""
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud SQL Instance Update"
  }

  display_name = "Cloud SQL Instance Update Detection"

  documentation {
    content   = "Verify that the Cloud SQL instance which was updated was actually meant to be updated, and what changed. If a change is unexpected or unwanted, file a ticket for further investigation or remediation.\n\nhttps://console.cloud.google.com/sql/instances?project=recidiviz-123"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.security.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "iam_changes_activities_cis" {
  alert_strategy {
    auto_close = "604800s"
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = "resource.type=\"global\" AND metric.type=\"logging.googleapis.com/user/iam_changes_activities_cis\""
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "iam_changes_activities_cis"
  }

  display_name          = "iam_changes_activities_cis"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    google_monitoring_notification_channel.security.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "gce_high_cpu_utilization" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      comparison      = "COMPARISON_GT"
      duration        = "1800s"
      filter          = <<-EOT
        metric.type="compute.googleapis.com/instance/cpu/utilization" resource.type="gce_instance"
      EOT
      threshold_value = "1"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "GCE: High CPU Utilization"
  }

  display_name = "GCE: High CPU Utilization"

  documentation {
    content   = "This alert means that disk utilization for a Compute Engine VM has reached an undesirably high percentage for an undesirably long duration of time. GCE VMs sit underneath of many of our services, including App Engine and Dataflow, so check the logs, App Engine dashboard, and Dataflow dashboard to examine platform activity.\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "vpc_network_changes" {
  alert_strategy {
    auto_close = "604800s"
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = "resource.type=\"global\" AND metric.type=\"logging.googleapis.com/user/vpc_network_changes\""
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "vpc_network_changes"
  }

  display_name          = "vpc_network_changes"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    google_monitoring_notification_channel.security.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "utah_data_transfer_sync_cloud_run_job" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_COUNT"
        group_by_fields      = ["resource.label.project_id", "resource.label.job_name"]
        per_series_aligner   = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "cloud_run_job" AND resource.labels.job_name = "utah-data-transfer-sync" AND metric.type = "run.googleapis.com/job/completed_task_attempt_count" AND metric.labels.result = "failed"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Utah Data Transfer Sync Cloud Run Job"
  }

  display_name = "Utah Data Transfer Sync Cloud Run Job"

  documentation {
    content   = "view runs at \nhttps://console.cloud.google.com/run/jobs/details/us-central1/utah-data-transfer-sync/executions?project=recidiviz-123\nor\nhttps://console.cloud.google.com/run/jobs/details/us-central1/utah-data-transfer-sync/executions?project=recidiviz-staging"
    mime_type = "text/markdown"
    subject   = "Utah Data Transfer Sync Cloud Run Job Failure"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.ie_on_call_general_alerts.id,
    google_monitoring_notification_channel.alerts.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "project_ownership_assignedchanged" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "600s"
        cross_series_reducer = "REDUCE_MAX"
        per_series_aligner   = "ALIGN_DELTA"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = "metric.type=\"logging.googleapis.com/user/project_owner_iam_change\""
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "New condition"
  }

  display_name          = "Project Ownership Assigned/Changed"
  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.security.id]
  project               = var.project_id
}


resource "google_monitoring_alert_policy" "arizona_oras_sheet_bq_scheduled_query_sync_monitoring" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED"]

    notification_rate_limit {
      period = "900s"
    }
  }

  combiner = "OR"

  conditions {
    condition_matched_log {
      filter = <<-EOT
        resource.type="bigquery_dts_config" severity="ERROR" resource.labels.config_id="690f2fec-0000-27b3-858e-747446fb41b8"
      EOT
    }

    display_name = "Log match condition"
  }

  display_name = "Arizona ORAS Sheet BQ Scheduled Query Sync Monitoring"

  documentation {
    content   = "The Arizona ORAS Sheet BQ Scheduled Query failed; see history at https://console.cloud.google.com/bigquery/transfers/locations/us/configs/690f2fec-0000-27b3-858e-747446fb41b8/runs?orgonly=true\u0026project=recidiviz-123\u0026supportedpurview=organizationId"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.ie_on_call_general_alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "app_engine_instance_memory_utilization_high" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      comparison      = "COMPARISON_GT"
      duration        = "60s"
      filter          = <<-EOT
        metric.type="agent.googleapis.com/memory/percent_used" resource.type="gce_instance" metric.label."state"="used"
      EOT
      threshold_value = "90"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "VM Instance - Memory utilization for used"
  }

  display_name = "App Engine: Instance Memory Utilization High"

  documentation {
    content   = "An instance of our appengine app has high memory utilization. If it hits the maximum memory the instance may become unresponsive."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_sql_high_storage_utilization" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        metric.type="cloudsql.googleapis.com/database/disk/utilization" resource.type="cloudsql_database"
      EOT
      threshold_value = "0.75"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud SQL: High Storage Utilization (default)"
  }

  display_name = "Cloud SQL: High Storage Utilization"

  documentation {
    content   = "This alert means that disk utilization for Cloud SQL has reached an undesirably high percentage. Visit the Cloud SQL console to determine if we need to expand the disk allotment for the instance in question: https://console.cloud.google.com/sql/instances?project=recidiviz-123\n\nCheck out the Database wiki page on pulse-data and/or pulse-data for information on database administration: https://github.com/Recidiviz/pulse-data/wiki\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D\n"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_function_failure" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MAX"
      }

      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields      = ["resource.label.project_id", "resource.label.function_name", "metric.label.status"]
        per_series_aligner   = "ALIGN_MAX"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "cloud_function" AND resource.labels.function_name != starts_with("test-") AND metric.type = "cloudfunctions.googleapis.com/function/execution_count" AND metric.labels.status != "ok"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud Function - Execution status was not `ok`"
  }

  display_name = "Cloud Function Failure"

  documentation {
    content   = "A Cloud Function failed to execute.\n\nCheck [recent logs in production](https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_function%22%0Aseverity%3DERROR;duration=PT96H?project=recidiviz-123)\nor [recent logs in staging](https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_function%22%0Aseverity%3DERROR;duration=PT96H?project=recidiviz-staging) for more information."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "gcs_metric_exports_have_not_been_uploaded_in_24_hours_2" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_monitoring_query_language {
      duration = "0s"
      query    = <<-EOT
        fetch k8s_cluster
        | metric
            'custom.googleapis.com/opencensus/metric_view_export_manager.export_file_age'
        | filter and(metric.metric_view_export_name != 'PO_MONTHLY', metric.metric_view_export_name != 'DASHBOARD_USER_RESTRICTIONS')
        | filter not(
            and(
                metric.metric_view_export_name = 'PUBLIC_DASHBOARD',
                or(
                    or(metric.region = 'US_ID', metric.region = 'US_ME'),
                    or(metric.region = 'US_TN', metric.region = 'US_IX')
                )
            )
        )
        | filter not(and(metric.metric_view_export_name = 'OVERDUE_DISCHARGE', metric.region = 'US_IX'))
        | align next_older(1h)
        # File age is a timestamp of seconds since epoch, so min() gives us the oldest file
        | group_by [resource.project_id, metric.region],
            [value_export_file_age_max: min(value.export_file_age)]
        | map [resource.project_id, metric.region],
            [value_export_file_age_max:
               cast_units(div(end(), 1s), 's') - cast_units(value_export_file_age_max, 's')]
        | condition gt(value_export_file_age_max, cast_units(60 * 60 * 28, 's'))
      EOT

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "File in metric view export is older than 24 hours"
  }

  display_name = "GCS: Metric exports have not been uploaded in 24 hours"

  documentation {
    content   = "See the files that violate the SLA at https://go/export-health-staging or https://go/export-health-prod\n\nThis may have been caused by upstream ingest processes failing to run, failing in an error state, or by an issue within the export pipeline. The metric captures GCS file age (in seconds)."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "metric_export_heartbeat_missing" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "3600s"
        cross_series_reducer = "REDUCE_COUNT"
        per_series_aligner   = "ALIGN_MEAN"
      }

      comparison              = "COMPARISON_LT"
      duration                = "7200s"
      evaluation_missing_data = "EVALUATION_MISSING_DATA_ACTIVE"
      filter                  = <<-EOT
        resource.type = "k8s_cluster" AND metric.type = "custom.googleapis.com/opencensus/metric_view_export_manager.export_file_age"
      EOT
      threshold_value         = "1"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Metric export heartbeat - No data received"
  }

  display_name = "GCS: Metric export monitoring data missing"

  documentation {
    content   = "The metric export file age metric has stopped being published. This could indicate:\n1. The metric export process has crashed or stopped running\n2. An OpenTelemetry configuration change (e.g., resource type change)\n3. A monitoring pipeline issue\n\nCheck the logs of the generate_export_timeliness_metrics task in the monitoring DAG and verify that metrics are being published to Cloud Monitoring. This is a meta-alert that detects when the monitoring system itself stops working."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "bq_deployed_view_too_expensive" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED"]

    notification_rate_limit {
      period = "3600s"
    }
  }

  combiner = "OR"

  conditions {
    condition_matched_log {
      filter = <<-EOT
        resource.type="k8s_container"
        resource.labels.container_name="base"
        textPayload=~"BigQueryViewDagWalker Node Failure"
      EOT
    }

    display_name = "Log match condition"
  }

  display_name = "BQ Deployed View Too Expensive"

  documentation {
    content   = "This alert fails when one or more views in our deployed BigQuery view DAG takes more than some threshold to update / materialize. Those thresholds are configured in recidiviz/big_query/view_update_config.py and views may be exempt on a case by case basis with the approval of Doppler."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_sql_high_cpu_utilization" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      comparison      = "COMPARISON_GT"
      duration        = "3600s"
      filter          = <<-EOT
        metric.type="cloudsql.googleapis.com/database/cpu/utilization" resource.type="cloudsql_database"
      EOT
      threshold_value = "1"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud SQL: High CPU Utilization"
  }

  display_name = "Cloud SQL: High CPU Utilization"

  documentation {
    content   = "This alert means that CPU utilization for Cloud SQL has persisted at an undesirably high rate for an undesirably long duration of time. Visit the Cloud SQL console to determine if we need to expand the CPU allotment for the instance in question: https://console.cloud.google.com/sql/instances?project=recidiviz-123\n\nCheck out the Database wiki page on pulse-data and/or pulse-data for information on database administration: https://github.com/Recidiviz/pulse-data/wiki\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

moved {
  from = google_monitoring_alert_policy.job_using_more_than_128_vcpu
  to =  google_monitoring_alert_policy.job_using_more_than_224_vcpu
}

resource "google_monitoring_alert_policy" "job_using_more_than_224_vcpu" {
  alert_strategy {
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields      = ["resource.label.project_id", "metadata.user_labels.\"dataflow_pipeline_job\"",]
        per_series_aligner   = "ALIGN_MAX"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "dataflow_job" AND resource.labels.job_name != monitoring.regex.full_match(".*test.*") AND metric.type = "dataflow.googleapis.com/job/current_num_vcpus" AND metadata.user_labels.dataflow_pipeline_name != monitoring.regex.full_match(".*supervision.*")
      EOT
      threshold_value = "224"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Dataflow Job - Current number of vCPUs in use"
  }

  display_name = "[Dataflow] Job using more than 224 vCPU"

  documentation {
    content   = "Triage with corresponding Doppler / Pod Tech pipeline owner"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
    google_monitoring_notification_channel.alerts.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "vpc_network_firewall_rule_changes_cis" {
  alert_strategy {
    auto_close = "604800s"
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_COUNT"
        per_series_aligner   = "ALIGN_COUNT"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type="global" AND metric.type="logging.googleapis.com/user/vpc_network_firewall_rule_changes_cis"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "vpc_network_firewall_rule_changes_cis"
  }

  display_name          = "vpc_network_firewall_rule_changes_cis"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    google_monitoring_notification_channel.security.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "airflow_dag_parse_error" {
  alert_strategy {
    auto_close           = "259200s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields      = ["resource.label.project_id", "resource.label.environment_name"]
        per_series_aligner   = "ALIGN_RATE"
      }

      comparison              = "COMPARISON_GT"
      duration                = "3600s"
      evaluation_missing_data = "EVALUATION_MISSING_DATA_ACTIVE"
      filter                  = <<-EOT
        resource.type = "cloud_composer_environment" AND resource.labels.environment_name != monitoring.regex.full_match("experiment.*") AND metric.type = "composer.googleapis.com/environment/dag_processing/parse_error_count"
      EOT
      threshold_value         = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud Composer Environment - DAG Parse Error Count"
  }


  display_name = "Airflow - DAG Parse Error"

  documentation {
    content   = "There has been an elevated rate of DAG parse errors. Verify that all DAGs are loaded and that there are no errors at go/airflow-staging and go/airflow-prod\n\nReview the following scheduler logs for any errors:\nprojects/recidiviz-staging/logs/airflow-scheduler\nprojects/${var.project_id}/logs/airflow-scheduler"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id
  ]
  project               = var.project_id
  severity              = "CRITICAL"
}

resource "google_monitoring_alert_policy" "cloud_composer_gke_container_cpu_usage_is_too_high" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "60s"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["metadata.system_labels.node_name"]
        per_series_aligner   = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "900s"
      filter          = <<-EOT
        resource.type = "k8s_container" AND metric.type = "kubernetes.io/container/cpu/core_usage_time"
      EOT
      threshold_value = "5"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Kubernetes Container - CPU usage time by metadata.system_labels.node_name [SUM]"
  }

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "300s"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.instance_id"]
        per_series_aligner   = "ALIGN_MEAN"
      }

      comparison      = "COMPARISON_GT"
      duration        = "300s"
      filter          = <<-EOT
        resource.type = "gce_instance" AND metric.type = "compute.googleapis.com/instance/cpu/utilization"
      EOT
      threshold_value = "1.2"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "GCE Instance - CPU utilization by label.instance_id [SUM]"
  }

  display_name = "Cloud Composer GKE Container CPU Usage is Too High"

  documentation {
    content   = "If you are unfamiliar with our Cloud Composer environments, please reach out to the #eng channel to notify the eng team that Composer is experiencing a prolonged spike in CPU usage.\n\nCloud Composer documentation: https://docs.google.com/document/d/1gh_KPdim7jnSO0Jd0RNm-VxUd9W6EQdHdLt5kgUggOI/edit"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_sql_high_write_frequency" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "1800s"
      filter          = <<-EOT
        resource.type = "cloudsql_database" AND metric.type = "cloudsql.googleapis.com/database/disk/write_ops_count"
      EOT
      threshold_value = "400"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud SQL: High Write Frequency"
  }

  display_name = "Cloud SQL: High Write Frequency"

  documentation {
    content   = "This alert means that disk write IO (writes per second) for Cloud SQL has spiked above an undesirably high level for an undesirably long duration of time. Visit the logs or the App Engine dashboard to examine platform activity.\n\nCheck out the Database wiki page on pulse-data and/or pulse-data for information on database administration: https://github.com/Recidiviz/pulse-data/wiki\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "datastore_high_entity_writes" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "60s"
        cross_series_reducer = "REDUCE_COUNT"
        per_series_aligner   = "ALIGN_DELTA"
      }

      aggregations {
        alignment_period   = "600s"
        per_series_aligner = "ALIGN_PERCENT_CHANGE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        metric.type="datastore.googleapis.com/entity/write_sizes" resource.type="datastore_request"
      EOT
      threshold_value = "500"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Datastore: High Entity Writes"
  }

  display_name = "Datastore: High Entity Writes"

  documentation {
    content   = "This alert means that entity write volume for Cloud Datastore has spiked unreasonably high too quickly. Datastore writes are performed by App Engine, so check the Datastore and App Engine dashboards to examine overall platform activity.\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "app_engine_too_many_serving_versions" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_monitoring_query_language {
      duration                = "21600s"
      evaluation_missing_data = "EVALUATION_MISSING_DATA_INACTIVE"
      query                   = <<-EOT
        fetch gae_instance
        | metric 'appengine.googleapis.com/flex/instance/cpu/usage_time'
        # For each instance that is reporting CPU utilization create a timeseries with the value of 1 per version id
        | group_by [resource.project_id, resource.module_id, resource.version_id], [serving_version: mean(1)]
        # Sum the number of serving versions
        | group_by [resource.project_id, resource.module_id], [serving_version_count: sum(serving_version)]
        | align next_older(1m)
        | every 1m
        | condition gt(serving_version_count, 1)
      EOT

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "GAE - Versions Being Served"
  }

  display_name = "App Engine: Too Many Serving Versions"

  documentation {
    content   = "Investigate why there are multiple versions being served and stop them if they are not needed."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_sql_high_read_frequency" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "600s"
      filter          = <<-EOT
        resource.type = "cloudsql_database" AND metric.type = "cloudsql.googleapis.com/database/disk/read_ops_count"
      EOT
      threshold_value = "150"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud SQL: High Read Frequency (non-state instance)"
  }

  display_name = "Cloud SQL: High Read Frequency"

  documentation {
    content   = "This alert means that disk read frequency (reads per second) for Cloud SQL has spiked above an undesirably high level for an undesirably long duration of time. Visit the logs or the App Engine dashboard to examine platform activity.\n\nCheck out the Database wiki page on pulse-data and/or pulse-data for information on database administration: https://github.com/Recidiviz/pulse-data/wiki\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "cloud_pubsub_subscription_message_not_acknowledged_within_an_hou" {
  alert_strategy {
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "120s"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields      = ["resource.label.subscription_id"]
        per_series_aligner   = "ALIGN_MAX"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "pubsub_subscription" AND metric.type = "pubsub.googleapis.com/subscription/oldest_unacked_message_age"
      EOT
      threshold_value = "3600"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Cloud Pub/Sub Subscription - Message not acknowledged within an hour"
  }

  display_name = "Cloud Pub/Sub Subscription - Message not acknowledged within an hour"

  documentation {
    content   = "Checks logs of relevant subscription handlers. For example, if the `storage-notifications-recidiviz-staging-practices-etl-data-workflows-firestore-etl` subscription is in escalation, check the logs of the staging `application-data-import` Cloud Run service"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
  severity              = "ERROR"
}

resource "google_monitoring_alert_policy" "cloud_iam_permission_audit_cis" {
  alert_strategy {
    auto_close = "604800s"
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = "resource.type=\"gcs_bucket\" AND metric.type=\"logging.googleapis.com/user/cloud_iam_permission_audit_cis\""
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "cloud_iam_permission_audit_cis"
  }

  display_name          = "cloud_iam_permission_audit_cis"
  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id, google_monitoring_notification_channel.security.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "potentially_idle_experiment_environment" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_prometheus_query_language {
      disable_metric_validation = "false"
      duration                  = "0s"
      evaluation_interval       = "3600s"
      query                     = <<-EOT
        (
            (
                # Take the current timestamp
                time() - 
                # Subtracted from the creation date of the environment
                max by (airflow_environment_name) (
                    last_over_time(
                    custom_googleapis_com:opencensus_airflow_environment_age{monitored_resource="generic_node", airflow_environment_name!~".+orchestration-v2"}[1h]
                    )
                )
            )
            # Divided by the seconds in the day
            / 86400
        )
        # Alert when days are greater than 10
        >= 10
        
        
      EOT
    }

    display_name = "Days since environment creation is greater than 10 days"
  }

  display_name = "[Airflow] Potentially idle experiment environment"

  documentation {
    content   = "Metric measures number of days since environment creation"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id
  ]
  project               = var.project_id
  severity              = "ERROR"
}

resource "google_monitoring_alert_policy" "datastore_high_entity_reads" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "60s"
        cross_series_reducer = "REDUCE_COUNT"
        per_series_aligner   = "ALIGN_DELTA"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        metric.type="datastore.googleapis.com/entity/read_sizes" resource.type="datastore_request"
      EOT
      threshold_value = "500"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Datastore: High Entity Reads"
  }

  display_name = "Datastore: High Entity Reads"

  documentation {
    content   = "This alert means that entity read volume for Cloud Datastore has spiked unreasonably high too quickly. Datastore reads are performed by App Engine, so check the Datastore and App Engine dashboards to examine overall platform activity.\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "firestore_high_write_frequency" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "900s"
      filter          = <<-EOT
        resource.type = "firestore_instance" AND metric.type = "firestore.googleapis.com/document/write_count"
      EOT
      threshold_value = "500"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Firestore: High Write Frequency"
  }

  display_name = "Firestore: High Write Frequency"

  documentation {
    content   = "This alert means that write operations (writes per second) for Firestore has spiked above an undesirably high level for an undesirably long duration of time. Visit the logs or the App Engine dashboard to examine platform activity.\n\nMake sure to log your work responding to this alert in the incident response log, and launch a retrospective if required by the issue: https://drive.google.com/drive/u/1/folders/1_o_-ZHnQ1qriXQH-3WtKLRBYPW8ODM3D"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}


resource "google_monitoring_alert_policy" "calculation_dag_has_not_triggered_within_timeframe" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "90000s"
        cross_series_reducer = "REDUCE_SUM"
        per_series_aligner   = "ALIGN_SUM"
      }

      comparison      = "COMPARISON_LT"
      duration        = "0s"
      filter          = "resource.type=\"cloud_composer_environment\" AND metric.type=\"logging.googleapis.com/user/recidiviz-123_calculation_dag_runs\""
      threshold_value = "1"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "New condition"
  }

  display_name = "Calculation DAG Has Not Triggered Within Timeframe"

  documentation {
    content   = "The calculations DAG run has not started within the selected time frame in production. Please check logs to determine if there are any errors causing this to happen."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id
  ]
  project               = var.project_id

  user_labels = {
    project_id = var.project_id
    severity   = "critical"
  }
}

resource "google_monitoring_alert_policy" "bq_scheduled_query_monitoring" {
  alert_strategy {
    notification_rate_limit {
      period = "900s"
    }
  }

  combiner = "OR"

  conditions {
    condition_matched_log {
      filter = "resource.type=\"bigquery_dts_config\" severity=\"ERROR\""
    }

    display_name = "Log match condition: failed scheduled query"
  }

  display_name = "BQ Scheduled Query Monitoring"

  documentation {
    content   = "A BQ scheduled query failed. See history at https://console.cloud.google.com/bigquery/transfers?project=recidiviz-123"
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id
  ]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "data_validation_failures_to_run" {
  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "86400s"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.project_id", "metric.label.validation_view_id"]
        per_series_aligner   = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "global" AND metric.type = "custom.googleapis.com/opencensus/recidiviz/validation/num_fail_to_run"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Global - OpenCensus/recidiviz/validation/num_fail_to_run"
  }

  display_name = "Data Validation Failures to Run"

  documentation {
    content   = "The data validation code failed to execute for all views. Check logs for failures in `/validation_manager/validate`."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alerts.id]
  project               = var.project_id
}

resource "google_monitoring_alert_policy" "uptime_check_url_down_marketing_website" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period     = "3600s"
        cross_series_reducer = "REDUCE_MEAN"
        group_by_fields      = ["metric.label.checked_resource_id"]
        per_series_aligner   = "ALIGN_FRACTION_TRUE"
      }

      comparison      = "COMPARISON_LT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "uptime_url" AND metric.type = "monitoring.googleapis.com/uptime_check/check_passed" AND metric.labels.checked_resource_id = "recidiviz.org"
      EOT
      threshold_value = "0.95"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "Uptime Check URL - Successful checks fell below 95%"
  }

  display_name          = "Uptime Check URL - DOWN (marketing website)"
  enabled               = "true"
  notification_channels = [
    google_monitoring_notification_channel.alerts.id,
    data.google_monitoring_notification_channel.pagerduty_alert_forwarder_service.id,
  ]
  project               = var.project_id
}
