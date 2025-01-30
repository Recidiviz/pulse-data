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

locals {
  # See: https://recidiviz.pagerduty.com/escalation_policies#PGUQH2D
  infrastructure_escalation_policy_id = "PGUQH2D"

  # See: https://recidiviz.pagerduty.com/escalation_policies#P6NIPW8
  reliability_escalation_policy_id = "P6NIPW8"

  # See: https://recidiviz.pagerduty.com/escalation_policies/P8VK0OC
  polaris_escalation_policy_id = "P8VK0OC"
}

module "data-platform-airflow-pagerduty-service" {
  source = "./modules/pagerduty-service"
  providers = {
    pagerduty = pagerduty
  }

  project_id                      = var.project_id
  service_base_name               = "Airflow Tasks: Data Platform"
  service_description             = "State-agnostic data platform infrastructure Airflow tasks."
  escalation_policy_id            = local.infrastructure_escalation_policy_id
  integration_email_base_username = "data-platform-airflow"
}

module "monitoring-airflow-pagerduty-service" {
  source = "./modules/pagerduty-service"
  providers = {
    pagerduty = pagerduty
  }

  project_id                      = var.project_id
  service_base_name               = "Airflow DAG: Monitoring"
  service_description             = "Airflow tasks / DAG responsible for generating PagerDuty alerts."
  escalation_policy_id            = local.reliability_escalation_policy_id
  integration_email_base_username = "monitoring-airflow"
  is_monitoring_service           = true
}

module "polaris-airflow-pagerduty-service" {
  source = "./modules/pagerduty-service"
  providers = {
    pagerduty = pagerduty
  }

  project_id                      = var.project_id
  service_base_name               = "Airflow Tasks: Polaris"
  service_description             = "Product-specific Polaris Airflow tasks"
  escalation_policy_id            = local.polaris_escalation_policy_id
  integration_email_base_username = "polaris-airflow"
}
