# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
# Definition for a PagerDuty service which is monitored / has alerts generated by our monitoring Airflow DAG.

# Required to use the pagerduty provider inside a module.
# See https://developer.hashicorp.com/terraform/language/modules/develop/providers.
terraform {
  required_providers {
    pagerduty = {
      source  = "PagerDuty/pagerduty"
      version = "3.11.4"
    }
  }
}

locals {
  project_name_str = var.project_id == "recidiviz-123" ? "PRODUCTION" : "STAGING"
  monitoring_dag_sendgrid_from_email_regex = "alerts\\+airflow-(staging|production)@recidiviz\\.org"
}

# Docs: https://registry.terraform.io/providers/PagerDuty/pagerduty/latest/docs/resources/service
resource "pagerduty_service" "service" {
  name                    = "[${local.project_name_str}] ${var.service_base_name}"
  description             = var.service_description
  escalation_policy       = var.escalation_policy_id

  acknowledgement_timeout = "null"
  auto_resolve_timeout    = "null"

  support_hours {
    # Monday to Friday support hours
    days_of_week = [1, 2, 3, 4, 5]
    start_time = "09:00:00"
    end_time   = "14:00:00"
    time_zone  = "America/Los_Angeles"
    type       = "fixed_time_per_day"
  }

  incident_urgency_rule {
    type = "use_support_hours"

    during_support_hours {
      type    = "constant"
      urgency = "high"
    }

    outside_support_hours {
      type    = "constant"
      urgency = "low"
    }
  }
}

# The defines an integration email that is designed to accept alert-generating email sent via a Sendgrid from our
# monitoring Airflow DAG.
# Docs: https://registry.terraform.io/providers/PagerDuty/pagerduty/latest/docs/resources/service_integration
resource "pagerduty_service_integration" "airflow_monitoring_email_integration" {
  name              = "Email"
  type              = "generic_email_inbound_integration"
  integration_email = "${var.integration_email_base_username}-${var.project_id}@recidiviz.pagerduty.com"
  service           = pagerduty_service.service.id
  email_incident_creation = "use_rules"
  email_filter_mode       = "and-rules-email"
  email_filter {
    body_mode        = "always"
    body_regex       = null
    from_email_mode  = "match"
    from_email_regex = local.monitoring_dag_sendgrid_from_email_regex
    subject_mode     = "always"
    subject_regex    = null
  }

  // If this service is the service that actually produces alerts, we want to fire alerts
  // for all failure emails, not just the correctly formatted ones.
  email_parsing_fallback  = var.is_monitoring_service ? "open_new_incident" : "discard"
  email_parser {
    action = "trigger"
    match_predicate {
      type = "any"
      predicate {
        matcher = "Task failure"
        part    = "subject"
        type    = "contains"
      }
    }
    value_extractor {
      part         = "subject"
      type         = "regex"
      regex = "Task failure: (.*)"
      value_name   = "incident_key"
    }
  }

  email_parser {
    action = "resolve"
    match_predicate {
      type = "any"
      predicate {
        matcher = "Task success"
        part    = "subject"
        type    = "contains"
      }
    }
    value_extractor {
      part       = "subject"
      type       = "regex"
      regex      = "Task success: (.*)"
      value_name = "incident_key"
    }
  }
}