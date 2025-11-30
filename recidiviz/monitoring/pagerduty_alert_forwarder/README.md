# Alert Forwarder Service

The Alert Forwarder service receives Cloud Monitoring alerts via Pub/Sub and forwards them to PagerDuty with configurable routing rules and transformations.

## Architecture

```
Cloud Monitoring Alert Policy
    ↓
Cloud Monitoring Notification Channel
    ↓
Pub/Sub Topic (cloud-monitoring-alerts-to-forward)
    ↓
Pub/Sub Subscription (push to Cloud Run)
    ↓
Cloud Run Service (pagerduty-alert-forwarder)
    ├─→ Rule Engine (evaluate & transform)
    ├─→ PagerDuty Events V2 API (forward incident)
    └─→ Dead Letter Queue (on failure, emails on-call)
```

## Configuration

### Rule Schema

Configuration is stored in `config.yaml`:

```yaml
# Default configuration for unmatched alerts
default:
  pagerduty_service: default-oncall  # Required: service to route to
  severity: info                      # Optional: default severity

# Rules evaluated in order; ALL matching rules are applied
rules:
  - name: "Human-readable rule name"
    match:
      # Match using JSON field paths with dot notation
      # All match criteria are optional but at least one required

      # Simple equality (case-insensitive)
      incident.severity: "ERROR"

      # Substring match (contains)
      incident.policy_name:
        contains: "database"

      # Value in list
      incident.severity:
        in: ["ERROR", "CRITICAL"]

    actions:
      # All actions are optional
      severity: critical                       # Override severity
      pagerduty_service: service-name         # Route to specific service
      title_prefix: "[PREFIX]"                # Add prefix to title
      title_suffix: "(suffix)"                # Add suffix to title
      title_transform: "Template with {incident.field}"  # Template with field interpolation
```

### Match Criteria

Match criteria use simple JSON field paths with dot notation (no indexing) to reference fields from the Cloud Monitoring alert:

**Match Types:**

1. **Simple Equality** (case-insensitive):
   ```yaml
   incident.severity: "ERROR"
   ```

2. **Contains** (case-insensitive substring match):
   ```yaml
   incident.policy_name:
     contains: "database"
   ```

3. **In List** (value must be in the list):
   ```yaml
   incident.severity:
     in: ["ERROR", "CRITICAL"]
   ```

**Available Fields:**

- `incident.policy_name`: Alert policy name
- `incident.resource_type_display_name`: Resource type (e.g., "cloud_run_revision", "cloud_sql_database")
- `incident.resource_display_name`: Resource name
- `incident.condition_name`: Condition that triggered
- `incident.severity`: Alert severity ("critical", "error", "warning", "info")
- `incident.summary`: Alert summary text
- `incident.incident_id`: Unique incident ID

The rest of the available fields can be found in the contents of the PagerDuty alert

**Important**:
- All specified criteria within a rule must match for that rule to apply (AND logic)
- **Multiple rules can match**: All matching rules are applied in order, with later rules potentially overriding earlier ones
- This allows for composable rule configuration (e.g., one rule routes by resource type, another adjusts severity based on environment)

### Actions

- **severity**: Override the PagerDuty severity (info/warning/error/critical)
- **pagerduty_service**: Route to a specific PagerDuty service (must be configured in secrets)
- **title_prefix**: Prepend text to the incident title
- **title_suffix**: Append text to the incident title
- **title_transform**: Replace the entire title with a template string using field interpolation

#### Title Transform Templates

The `title_transform` action allows you to completely replace the incident title with a custom template that can reference fields from the Cloud Monitoring alert data using `{field.path}` syntax.

**Available fields** (from the `incident` object):
- `{incident.policy_name}`: Alert policy name
- `{incident.resource_type_display_name}`: Resource type (e.g., "cloud_run_revision", "cloud_sql_database")
- `{incident.resource_display_name}`: Resource name
- `{incident.condition_name}`: Condition that triggered
- `{incident.summary}`: Alert summary text
- `{incident.severity}`: Original alert severity
- `{incident.incident_id}`: Unique incident ID

**Examples**:

```yaml
# Simple template with resource info
title_transform: "[{incident.resource_type_display_name}] {incident.summary}"
# Result: "[cloud_run_revision] High error rate detected"

# Include resource name and summary
title_transform: "{incident.resource_display_name}: {incident.summary}"
# Result: "my-service-prod: High error rate detected"

# Fully custom format
title_transform: "[PROD] {incident.resource_type_display_name}/{incident.resource_display_name}: {incident.summary}"
# Result: "[PROD] cloud_run_revision/my-service-prod: High error rate detected"
```

**Note**: When `title_transform` is specified, it completely replaces the title. The `title_prefix` and `title_suffix` actions are ignored if `title_transform` is present.

### Rule Composition

One of the most powerful features is that **multiple rules can match and be applied in sequence**. This allows you to compose alert routing logic from simple, reusable rules.

**Example - Layered Configuration**:

```yaml
rules:
  # Base rule: Route all database alerts to database team
  - name: "Database Resource Routing"
    match:
      incident.resource_type_display_name:
        contains: "database"
    actions:
      pagerduty_service: database-oncall

  # Layer 1: Production resources get critical severity
  - name: "Production Critical"
    match:
      incident.resource_display_name:
        contains: "prod"
    actions:
      severity: critical
      title_prefix: "[PROD]"

  # Layer 2: But ERROR severity alerts get downgraded to error (not critical)
  - name: "Error Severity"
    match:
      incident.severity: "ERROR"
    actions:
      severity: error  # Overrides the critical from previous rule
```

In this example, an alert for a production database with ERROR severity would:
1. Match rule 1 → Route to `database-oncall`
2. Match rule 2 → Set severity to `critical`, add `[PROD]` prefix
3. Match rule 3 → Override severity to `error`

**Final result**: Routed to `database-oncall` with `error` severity and `[PROD]` prefix.

**Use Cases for Rule Composition**:
- **Separation of concerns**: One set of rules for routing, another for severity, another for title formatting
- **Environment-based overrides**: Base rules for all resources, override rules for production/staging
- **Team-based routing**: Route by resource type, but override for specific high-priority resources
- **Progressive enhancement**: Start with broad rules, add specific overrides as needed

**Important**: Since later rules override earlier ones, order your rules from most general to most specific.

## Deployment

### Prerequisites

1. **Docker Image**: Build and push the Docker image to Artifact Registry (only needed when the source code changes)
   ```bash
   cd /path/to/pulse-data
   docker build -f recidiviz/monitoring/pagerduty_alert_forwarder/Dockerfile \
     -t us-docker.pkg.dev/recidiviz-123/pagerduty-alert-forwarder/pagerduty-alert-forwarder:latest . \
     --platform linux/amd64
   docker push us-docker.pkg.dev/recidiviz-123/pagerduty-alert-forwarder/pagerduty-alert-forwarder:latest
   ```

### Terraform Deployment

1. **Update Configuration** (if needed):
   Edit `recidiviz/monitoring/pagerduty_alert_forwarder/config.yaml` to add/modify rules

2. **Deploy with Atmos**:
   ```bash
   atmos terraform apply apps/pagerduty-alert-forwarder --stack recidiviz-123
   ```

### Linking Cloud Monitoring

To send alerts to the forwarder, configure a Cloud Monitoring notification channel:

1. **Get the Pub/Sub topic name**:
   ```bash
   atmos terraform output apps/pagerduty-alert-forwarder --stack recidiviz-123
   # Note the pubsub_topic_name
   ```

2. **Create notification channel** (via Terraform or Console):
   ```hcl
   resource "google_monitoring_notification_channel" "pagerduty_forwarder" {
     display_name = "PagerDuty via Alert Forwarder"
     type         = "pubsub"

     labels = {
       topic = "projects/recidiviz-123/topics/cloud-monitoring-alerts-to-forward"
     }
   }
   ```

3. **Add to alert policies**:
   ```hcl
   resource "google_monitoring_alert_policy" "example" {
     # ... alert configuration ...

     notification_channels = [
       google_monitoring_notification_channel.pagerduty_forwarder.id
     ]
   }
   ```

## Monitoring & Operations

### Health Checks

- **Health endpoint**: `https://<service-url>/health`
- **Cloud Run health probes**: Configured for liveness and startup

### Logs

View logs in Cloud Logging:
```bash
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="pagerduty-alert-forwarder"' \
  --project=recidiviz-123 \
  --limit=50
```

Structured log fields:
- `alert_name`: Cloud Monitoring alert policy name
- `rule_name`: Matched rule name (if any)
- `service`: PagerDuty service name
- `severity`: PagerDuty severity
- `dedup_key`: PagerDuty deduplication key

### Dead Letter Queue

Messages that fail after 5 retry attempts are sent to the DLQ.

**View DLQ messages**:
```bash
gcloud pubsub subscriptions pull pagerduty-alert-forwarder-dlq-pull \
  --project=recidiviz-123 \
  --limit=10 \
  --format=json
```

**DLQ Alert**: When messages appear in the DLQ, an alert is triggered and sent to `alerts@recidiviz.org`.
If the PagerDuty Alert Forwarder service not failing for all messages, it may also be forwarded to PagerDuty / Slack.

**Common failure causes**:
- Invalid PagerDuty integration key
- PagerDuty API outage
- Malformed alert data
- Missing configuration for service


**Remediation**:
After identifying the underlying cause as to why the message was not processed, determine the following:
* If the message contained a valid alert, fix the underlying issue then replay the message using `./tools/replay_dead_letter_queue.sh`
  * Replayed messages will make their way back into the dead letter queue if the fail to process again
* Otherwise, if the message was invalid (test / dummy data), discard the message by acknowledging it in the `Messages` tab of the
`pagerduty-alert-forwarder-dlq-pull` Pub/Sub subscription.

### Metrics

Key metrics to monitor (available in Cloud Console):

- **Request count**: `run.googleapis.com/request_count`
- **Request latency**: `run.googleapis.com/request_latencies`
- **Container CPU**: `run.googleapis.com/container/cpu/utilizations`
- **DLQ depth**: `pubsub.googleapis.com/subscription/num_undelivered_messages`

## Testing

### Unit Tests

Run unit tests:
```bash
pytest recidiviz/tests/monitoring/pagerduty_alert_forwarder/
```

### Integration Testing

1. **Test with sample alert**:
   ```bash
   # Create a test alert payload
   cat > /tmp/test-alert.json <<EOF
   {
     "incident": {
       "incident_id": "test-123",
       "policy_name": "Test Database Alert",
       "resource_type_display_name": "cloud_sql_database",
       "resource_display_name": "production-db",
       "condition_name": "High connection errors",
       "summary": "Database connection errors detected",
       "severity": "ERROR",
       "url": "https://console.cloud.google.com/monitoring"
     }
   }
   EOF

   # Publish to Pub/Sub
   gcloud pubsub topics publish cloud-monitoring-alerts-to-forward \
     --message="$(cat /tmp/test-alert.json)" \
     --project=recidiviz-123
   ```

2. **Verify in PagerDuty**: Check that incident was created with expected severity and title

3. **Check logs**: Verify processing in Cloud Logging

## Troubleshooting

### No incidents created in PagerDuty

1. Check Cloud Run logs for errors
2. Verify service is receiving messages: check request count metrics
3. Verify PagerDuty integration keys are correctly set in Secret Manager
4. Test PagerDuty API directly with curl

### Incidents created with wrong severity/service

1. Check rule configuration in `config.yaml`
2. Verify rule matching logic in logs (look for "Alert matched rule" messages)
3. Test rule engine with sample alert using unit tests

### High DLQ depth

1. Check DLQ alert notification email
2. Pull messages from DLQ to inspect failure reasons
3. Check Cloud Run logs for error patterns
4. Verify PagerDuty service is operational

### Configuration changes not applied

1. Rebuild and push Docker image with updated config.yaml
2. Redeploy Cloud Run service: `atmos terraform apply apps/pagerduty-alert-forwarder --stack recidiviz-123`
3. Verify new revision is serving traffic in Cloud Console

## Development

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r recidiviz/monitoring/pagerduty_alert_forwarder/requirements.txt
   ```

2. **Set environment variables**:
   ```bash
   export GCP_PROJECT=recidiviz-staging
   export PAGERDUTY_SERVICES=default-oncall,test-service
   export CONFIG_PATH=/path/to/config.yaml
   ```

3. **Run locally**:
   ```bash
   python -m recidiviz.monitoring.pagerduty_alert_forwarder.server
   ```

4. **Test endpoint**:
   ```bash
   curl -X POST http://localhost:8080/pubsub-push \
     -H "Content-Type: application/json" \
     -d '{
       "message": {
         "data": "'"$(echo '{"incident":{"policy_name":"test"}}' | base64)"'"
       }
     }'
   ```

### Adding New Rules

1. Edit `recidiviz/monitoring/pagerduty_alert_forwarder/config.yaml`
2. Add new rule to the `rules` list
3. Test with unit tests if complex pattern matching
4. Rebuild Docker image
5. Deploy with Atmos
6. Verify with test alert

### Adding New PagerDuty Service

1. Add service name to `pagerduty_services` in stack config
2. Deploy with Atmos (creates secret containing integration key)
3. Update `config.yaml` to route alerts to new service
