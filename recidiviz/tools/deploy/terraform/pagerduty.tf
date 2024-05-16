# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

// These import blocks can be deleted once this code has been deployed to production
import {
  to = module.data-platform-airflow-pagerduty-service.pagerduty_service.service
  id = "P1A7TVN"
  # Only import if project is staging, otherwise create a new service.
  for_each = var.project_id == "recidiviz-staging" ? [0] : []
}

import {
  to = module.data-platform-airflow-pagerduty-service.pagerduty_service_integration.airflow_monitoring_email_integration
  id = "P1A7TVN.PFGP7NF"
  # Only import if project is staging, otherwise create a new integration.
  for_each = var.project_id == "recidiviz-staging" ? [0] : []
}

import {
  to = module.monitoring-airflow-pagerduty-service.pagerduty_service.service
  id = "P279952"
  # Only import if project is staging, otherwise create a new service.
  for_each = var.project_id == "recidiviz-staging" ? [0] : []
}

import {
  to = module.monitoring-airflow-pagerduty-service.pagerduty_service_integration.airflow_monitoring_email_integration
  id = "P279952.PF953FR"
  # Only import if project is staging, otherwise create a new integration.
  for_each = var.project_id == "recidiviz-staging" ? [0] : []
}

locals {
  # See: https://recidiviz.pagerduty.com/escalation_policies#PGUQH2D
  infrastructure_escalation_policy_id = "PGUQH2D"

  # See: https://recidiviz.pagerduty.com/escalation_policies#P6NIPW8
  reliability_escalation_policy_id = "P6NIPW8"
}

module "data-platform-airflow-pagerduty-service" {
  source = "./modules/pagerduty-service"
  providers = {
    pagerduty = pagerduty
  }

  project_id           = var.project_id
  service_base_name    = "Airflow Tasks: Data Platform"
  service_description  = "State-agnostic data platform infrastructure Airflow tasks."
  escalation_policy_id = local.infrastructure_escalation_policy_id
  # TODO(#28642): Change this to "data-platform-airflow" when the routing code is updated
  #  to use the email defined in RecidivizPagerDutyService
  integration_email_username = var.project_id == "recidiviz-123" ? "data-platform-airflow-${var.project_id}" : "calculation-dag-email.b1cp4to2"
}

module "monitoring-airflow-pagerduty-service" {
  source = "./modules/pagerduty-service"
  providers = {
    pagerduty = pagerduty
  }

  project_id           = var.project_id
  service_base_name    = "Airflow DAG: Monitoring"
  service_description  = "Airflow tasks / DAG responsible for generating PagerDuty alerts."
  escalation_policy_id = local.reliability_escalation_policy_id
  # TODO(#28642): Change this to "monitoring-airflow" when the routing code is updated
  #  to use the email defined in RecidivizPagerDutyService
  integration_email_username = var.project_id == "recidiviz-123" ? "data-platform-airflow-${var.project_id}" : "airflow-dag--monitoring-email.6ai6cy3n"
  is_monitoring_service      = true
}
