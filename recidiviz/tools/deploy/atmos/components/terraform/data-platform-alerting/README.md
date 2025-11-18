# Terraform Component: Data Platform Alerting

This Terraform component manages Google Cloud Monitoring alert policies and notification channels for the Recidiviz data platform.
It provides a centralized configuration for all monitoring alerts across production and staging environments.

## Overview

This component provisions and manages:
1. **Alert Policies**: Monitoring alert policies for various GCP services and custom metrics
2. **Notification Channels**: Email, PagerDuty, and Slack notification channels for alert delivery

All alert policies are defined as code, enabling version control, peer review, and reproducible infrastructure.

## Features

This component manages alerts for:
- **Cloud SQL**: CPU, memory, storage utilization, and I/O operations
- **Compute Engine**: High CPU utilization
- **App Engine**: Memory utilization and version management
- **Cloud Run**: Job failures across multiple projects
- **Dataflow**: vCPU usage monitoring
- **Cloud Functions**: Execution failures
- **Cloud Composer/Airflow**: DAG parse errors and environment monitoring
- **BigQuery**: View update failures, scheduled query monitoring
- **Cloud Storage**: Metric export monitoring
- **Firestore/Datastore**: Read/write frequency monitoring
- **Pub/Sub**: Message acknowledgment delays
- **Security/CIS Compliance**: IAM changes, VPC network changes, audit configuration changes
- **Uptime Checks**: URL availability monitoring

## Requirements

- **Terraform Version**: 1.0 or later
- **Google Provider Version**: ~> 6.50.0
- **GCP Permissions**: The user or service account must have permissions to:
  - Create and manage Monitoring Alert Policies
  - Create and manage Notification Channels
  - Read project resources

## File Structure

```
data-platform-alerting/
├── monitoring_alert_policy.tf          # Alert policy definitions
├── monitoring_notification_channel.tf  # Notification channel definitions
├── providers.tf                         # Terraform and provider configuration
└── recidiviz-123-data-platform-alerting.terraform.tfvars.json  # Generated variables
```

## Usage with Atmos

This component is deployed using [Atmos](https://atmos.tools/introduction/). All `atmos` commands must be run from the Atmos root directory:

```bash
cd recidiviz/tools/deploy/atmos
```

### Common Operations

```bash
# Plan changes for production
atmos terraform plan data-platform-alerting -s recidiviz-123

# Apply changes to production
atmos terraform apply data-platform-alerting -s recidiviz-123

# View current state
atmos terraform show data-platform-alerting -s recidiviz-123

# Modify the encrypted config secrets for use in a resource
# Access to this file is always-on for on-call, doppler, and aurora
# Use go/jit for recidiviz-123 if an access error is raised when running this command
sops recidiviz/tools/deploy/atmos/components/terraform/data-platform-alerting/config.production.enc.yaml
```


## Creating a New Alert Policy

### Option 1: Define Alert in Terraform (Recommended)

1. Add a new `google_monitoring_alert_policy` resource to `monitoring_alert_policy.tf`
2. Follow the existing pattern and include:
   - Descriptive `display_name`
   - Appropriate `conditions` based on metrics or logs
   - `notification_channels` references
   - Optional `documentation` block with runbook information
   - `enabled` flag (typically `true`)
   - `project` variable reference

Example:
```hcl
resource "google_monitoring_alert_policy" "my_new_alert" {
  alert_strategy {
    auto_close           = "604800s"
    notification_prompts = ["OPENED", "CLOSED"]
  }

  combiner = "OR"

  conditions {
    condition_threshold {
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_RATE"
      }

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      filter          = <<-EOT
        resource.type = "cloud_run_job" AND metric.type = "run.googleapis.com/job/completed_task_attempt_count"
      EOT
      threshold_value = "0"

      trigger {
        count   = "1"
        percent = "0"
      }
    }

    display_name = "My Alert Condition"
  }

  display_name = "My New Alert"

  documentation {
    content   = "Runbook for responding to this alert..."
    mime_type = "text/markdown"
  }

  enabled               = "true"
  notification_channels = [google_monitoring_notification_channel.alertsrecidiviz_org.id]
  project               = var.project_id
}
```

3. Run `atmos terraform plan data-platform-alerting -s recidiviz-123` to preview changes
4. Run `atmos terraform apply data-platform-alerting -s recidiviz-123` to create the alert

### Option 2: Create in UI and Import to Terraform

Sometimes it's easier to prototype an alert using the Google Cloud Console UI, then import it to Terraform.

#### Step 1: Create Alert Policy in Google Cloud Console

1. Navigate to the [Cloud Monitoring Alerting page](https://console.cloud.google.com/monitoring/alerting) for your project
2. Click **+ CREATE POLICY**
3. Configure your alert:
   - **What do you want to track?**: Define conditions (metric, threshold, duration)
   - **Who should be notified?**: Select notification channels
   - **What are the steps to fix the issue?**: Add documentation/runbook
   - **Name this policy**: Give it a descriptive name
4. Click **CREATE POLICY**
5. Note the **Alert Policy Name** from the policy details page

#### Step 2: Find the Alert Policy ID

Option A - From the Console:
1. Go to the [Alerting Policies](https://console.cloud.google.com/monitoring/alerting/policies) page
2. Click on your newly created policy
3. The policy ID is in the URL: `https://console.cloud.google.com/monitoring/alerting/policies/[POLICY_ID]`

Option B - Using gcloud:
```bash
# List all alert policies
gcloud alpha monitoring policies list --project=recidiviz-123

# Find your policy by name and note the policy ID
```

#### Step 3: Import the Alert Policy to Terraform

1. Add a placeholder resource definition to `monitoring_alert_policy.tf`:

```hcl
resource "google_monitoring_alert_policy" "my_imported_alert" {
  # Placeholder - will be populated by import
  display_name = "My Imported Alert"
  combiner     = "OR"
  enabled      = true
  project      = var.project_id
}
```

2. Import the existing alert policy using Atmos:

```bash
cd recidiviz/tools/deploy/atmos

# Import the alert policy
atmos terraform import \
  data-platform-alerting \
  -s recidiviz-123 \
  google_monitoring_alert_policy.my_imported_alert \
  projects/recidiviz-123/alertPolicies/[POLICY_ID]
```

3. Generate the Terraform configuration from the imported state:

```bash
# Show the terraform state and search for the imported resource configuration
atmos terraform show data-platform-alerting -s recidiviz-123
```

4. Update `monitoring_alert_policy.tf` with the actual configuration from the import:
   - Copy the resource configuration from `terraform show` output
   - Clean up any computed or optional fields that are set to defaults
   - Ensure notification channels reference Terraform resources (not hardcoded IDs)
   - Add helpful comments and documentation

5. Verify the configuration matches:

```bash
cd ../../..  # Back to atmos root
atmos terraform plan data-platform-alerting -s recidiviz-123
```

The plan should show **no changes** if the import was successful.

6. **(Optional)** Delete the original alert from the UI if you want to ensure Terraform is the source of truth, then apply:

```bash
atmos terraform apply data-platform-alerting -s recidiviz-123
```

## Notification Channels

Notification channels are defined in `monitoring_notification_channel.tf`. Available channels include:

- **Email**: `alertsrecidiviz_org`, `securityrecidiviz_org`
- **PagerDuty**: `stackdriver`, `polaris_general_infrastructure`, `ie_on_call_general_alerts`
- **Slack**: `security_alerts`, `justice_counts_gcp_errors`

To reference a notification channel in an alert policy:
```hcl
notification_channels = [
  google_monitoring_notification_channel.alertsrecidiviz_org.id,
  google_monitoring_notification_channel.stackdriver.id
]
```

## Best Practices

1. **Documentation**: Always include a `documentation` block with:
   - Clear description of what the alert means
   - Steps to investigate and resolve
   - Links to relevant dashboards, logs, or runbooks

2. **Naming**: Use descriptive names that indicate:
   - The service/resource being monitored
   - The condition being alerted on
   - Example: `cloud_sql_high_cpu_utilization`

3. **Thresholds**: Set thresholds and durations that:
   - Minimize false positives
   - Provide early warning without being too sensitive
   - Are based on historical data and SLOs

4. **Testing**: After creating/modifying alerts:
   - Verify the alert triggers correctly in staging first
   - Test notification delivery
   - Ensure documentation is clear and actionable

5. **Notification Channels**:
   - Use appropriate channels for alert severity
   - Critical alerts → PagerDuty
   - Warnings → Email/Slack
   - Security alerts → Dedicated security channels

6. **Auto-close**: Configure `auto_close` periods to prevent stale alert notifications

## Troubleshooting

### Import Fails
- Verify the policy ID is correct
- Ensure you have proper permissions in the GCP project
- Check that the resource name in Terraform doesn't already exist

### Plan Shows Unexpected Changes
- Some fields are computed and may show changes on first plan
- Notification channels may need to be referenced by resource ID
- Remove any sensitive values (like PagerDuty service keys) - these should be `null` in config

### Alert Not Triggering
- Check the metric exists and has recent data
- Verify the filter syntax is correct
- Review alignment period and threshold values
- Test the alert condition in Metrics Explorer first

## References

- [Google Cloud Monitoring Documentation](https://cloud.google.com/monitoring/docs)
- [Terraform Google Provider - Alert Policy](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/monitoring_alert_policy)
- [Atmos Documentation](https://atmos.tools/introduction/)
- [Recidiviz Atmos Setup](../../README.md)
