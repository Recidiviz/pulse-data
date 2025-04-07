# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View containing transition events associated with the PSI Case Insights tool
for org-wide impact tracking"""

from recidiviz.common.decarceral_impact_type import DecarceralImpactType
from recidiviz.outcome_metrics.impact_transitions_view_builder import (
    ImpactTransitionsBigQueryViewBuilder,
)
from recidiviz.outcome_metrics.product_transition_type import ProductTransitionType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """View containing transition events associated with the PSI Case Insights tool
for org-wide impact tracking"""

_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH transitions AS (
    SELECT
        person_id,
        sentence_start_date AS event_date,
        state_code
    FROM
        `{{project_id}}.sentencing_views.sentencing_case_disposition_materialized`
    WHERE
        disposition = "PROBATION"
)
SELECT
    transitions.*,
    launches.experiment_id,
    -- The flags below are only relevant to the Workflows product, so we leave them
    -- as null.
    "{DecarceralImpactType.SENTENCE_TO_PROBATION.value}" AS decarceral_impact_type,
    FALSE AS has_mandatory_due_date,
    FALSE AS is_jii_transition,
    "PRE-SENTENCING" AS system_type,
    launches.launch_date AS full_state_launch_date,
FROM
    transitions
INNER JOIN
    `{{project_id}}.transitions.all_full_state_launch_dates_materialized` launches
ON
    transitions.state_code = launches.state_code
    AND launches.experiment_id LIKE "%PSI_CASE_INSIGHTS%"
"""

VIEW_BUILDER: ImpactTransitionsBigQueryViewBuilder = (
    ImpactTransitionsBigQueryViewBuilder(
        description=_VIEW_DESCRIPTION,
        query_template=_SOURCE_DATA_QUERY_TEMPLATE,
        weight_factor=1,
        delta_direction_factor=1,
        product_transition_type=ProductTransitionType.PSI_CASE_INSIGHTS,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
