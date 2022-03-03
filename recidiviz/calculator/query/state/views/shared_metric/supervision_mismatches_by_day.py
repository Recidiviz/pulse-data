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
"""People on supervision eligible for supervision downgrade by PO by day."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_MISMATCHES_BY_DAY_VIEW_NAME = "supervision_mismatches_by_day"

SUPERVISION_MISMATCHES_BY_DAY_DESCRIPTION = """
Number of people on supervision who can have their supervision level downgraded by day.
Indexed by supervising officer and associated district-office to support aggregation.
"""

SUPERVISION_MISMATCHES_BY_DAY_QUERY_TEMPLATE = """
/*{description}*/
SELECT
    compliance.state_code,
    compliance.date_of_supervision,
    compliance.person_id,
    supervising_officer_external_id,
    IFNULL(level_1_supervision_location_external_id, 'UNKNOWN') as level_1_supervision_location_external_id,
    IFNULL(level_2_supervision_location_external_id, 'UNKNOWN') as level_2_supervision_location_external_id,
    recommended_supervision_downgrade_level
FROM `{project_id}.{shared_metric_views_dataset}.supervision_case_compliance_metrics_materialized` compliance,
UNNEST ([compliance.level_1_supervision_location_external_id, 'ALL']) AS level_1_supervision_location_external_id,
UNNEST ([compliance.level_2_supervision_location_external_id, 'ALL']) AS level_2_supervision_location_external_id,
UNNEST ([supervising_officer_external_id, 'ALL']) AS supervising_officer_external_id
-- Remove duplicate entries created when unnesting a state that does not have L2 locations
WHERE level_2_supervision_location_external_id IS NOT NULL
"""

SUPERVISION_MISMATCHES_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SUPERVISION_MISMATCHES_BY_DAY_VIEW_NAME,
    view_query_template=SUPERVISION_MISMATCHES_BY_DAY_QUERY_TEMPLATE,
    description=SUPERVISION_MISMATCHES_BY_DAY_DESCRIPTION,
    should_materialize=True,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_MISMATCHES_BY_DAY_VIEW_BUILDER.build_and_print()
