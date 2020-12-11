# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Shared violation reports query, used for querying absconsions and revocations by person by month for the PO report.
"""

# TODO(#4155): Use the supervision_termination_metrics instead of the raw state_supervision_period table
def violation_reports_query(state_dataset: str, reference_views_dataset: str) -> str:
    """Returns a query for violation reports where the officer_external_id is not null and the associated
    supervision period is of type 'DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN'.
    """
    return f"""
      SELECT
        violation.state_code,
        EXTRACT(YEAR FROM violation.response_date) AS year,
        EXTRACT(MONTH FROM violation.response_date) AS month,
        violation.person_id,
        type.violation_type,
        violation.response_date,
        decision.decision AS response_decision,
        agent.agent_external_id AS officer_external_id
      FROM `{{project_id}}.{state_dataset}.state_supervision_violation_response` violation
      LEFT JOIN `{{project_id}}.{state_dataset}.state_supervision_violation_response_decision_entry` decision
        USING (supervision_violation_response_id, person_id, state_code)
      LEFT JOIN `{{project_id}}.{state_dataset}.state_supervision_violation_type_entry` type
        USING (supervision_violation_id, person_id, state_code)
      LEFT JOIN `{{project_id}}.{state_dataset}.state_supervision_period` period
        -- Find the overlapping supervision periods for this violation report
        ON period.person_id = violation.person_id
            AND period.state_code = violation.state_code
            AND violation.response_date >= period.start_date
            AND violation.response_date <= COALESCE(period.termination_date, '9999-12-31')
      LEFT JOIN `{{project_id}}.{reference_views_dataset}.supervision_period_to_agent_association` agent
        ON period.supervision_period_id = agent.supervision_period_id
          AND period.state_code = agent.state_code
      WHERE period.supervision_period_supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
        AND agent.agent_external_id IS NOT NULL
    """
