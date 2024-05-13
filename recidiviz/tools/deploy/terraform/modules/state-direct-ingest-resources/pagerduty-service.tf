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
   # See: https://recidiviz.pagerduty.com/escalation_policies#P7Y4PYG
   implementation_engineer_escalation_policy_id = "P7Y4PYG"
}

module "pagerduty-service-airflow" {
  source = "../pagerduty-service-airflow"

  project_id = var.project_id
  service_base_name                = "Airflow Tasks: ${var.state_code}"
  service_description         = "Airflow tasks that do data processing specific to ${var.state_code}."
  escalation_policy_id        = local.implementation_engineer_escalation_policy_id
  integration_email_username  = "${local.lower_state_code}-airflow-${var.project_id}"
  email_parsing_fallback      = "discard"
}
