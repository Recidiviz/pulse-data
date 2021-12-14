#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
"""Periods when people were overdue for discharge from supervision.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.reference.overdue_discharge_outcomes
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_supervision_id_types = PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE[
    "supervision"
].values()

OVERDUE_DISCHARGE_OUTCOMES_VIEW_NAME = "overdue_discharge_outcomes"

OVERDUE_DISCHARGE_OUTCOMES_DESCRIPTION = """Periods when people were overdue for discharge
from supervision, including the dates when this was reported to corrections staff and when
the person was ultimately discharged (where applicable)."""

OVERDUE_DISCHARGE_OUTCOMES_QUERY_TEMPLATE = f"""
SELECT 
    day_zero_reports.state_code,
    pei.person_id,
    # there can only be one report_date per supervision period; it represents the earliest date
    # at which we surfaced a person's overdue status to state corrections staff via tool or report.
    report_date,
    end_date AS discharge_date,
    outflow_to_level_1 AS discharge_outflow
FROM `{{project_id}}.{{static_reference_dataset}}.day_zero_reports` day_zero_reports
# inner join here in case we somehow wind up with an ID in day_zero_reports that isn't actually in our data;
# those people will be excluded from the final output of this view
INNER JOIN `{{project_id}}.{{state_dataset}}.state_person_external_id` pei
    ON day_zero_reports.state_code = pei.state_code
    AND day_zero_reports.person_external_id = pei.external_id
    AND pei.id_type IN {tuple(_supervision_id_types)}
LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` compartment_sessions
    ON day_zero_reports.state_code = compartment_sessions.state_code
    AND pei.person_id = compartment_sessions.person_id
    AND report_date BETWEEN start_date AND IFNULL(end_date, '9999-01-01')
WHERE opportunity_type = '{OpportunityType.OVERDUE_DISCHARGE.value}'
    # discard any reports where we don't actually show the person as having been on supervision at the time
    # (could have happened due to error, reporting lag, backdating, etc)
    AND compartment_level_1 = "SUPERVISION"
"""

OVERDUE_DISCHARGE_OUTCOMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=OVERDUE_DISCHARGE_OUTCOMES_VIEW_NAME,
    view_query_template=OVERDUE_DISCHARGE_OUTCOMES_QUERY_TEMPLATE,
    description=OVERDUE_DISCHARGE_OUTCOMES_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERDUE_DISCHARGE_OUTCOMES_VIEW_BUILDER.build_and_print()
