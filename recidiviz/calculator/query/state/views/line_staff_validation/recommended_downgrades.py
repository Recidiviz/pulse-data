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
"""Recommended downgrade data

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.recommended_downgrades
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.views import dataset_config as case_triage_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RECOMMENDED_DOWNGRADES_VIEW_NAME = "recommended_downgrades"

RECOMMENDED_DOWNGRADES_VIEW_NAME_DESCRIPTION = """
"""

RECOMMENDED_DOWNGRADES_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_external_id,
    JSON_EXTRACT_SCALAR(opportunity_metadata, '$.recommendedSupervisionLevel') AS recommended_supervision_level
FROM `{project_id}.{case_triage_dataset}.etl_opportunities_materialized`
WHERE opportunity_type = 'OVERDUE_DOWNGRADE';
"""

RECOMMENDED_DOWNGRADES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=RECOMMENDED_DOWNGRADES_VIEW_NAME,
    should_materialize=True,
    view_query_template=RECOMMENDED_DOWNGRADES_QUERY_TEMPLATE,
    description=RECOMMENDED_DOWNGRADES_VIEW_NAME_DESCRIPTION,
    case_triage_dataset=case_triage_dataset_config.VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RECOMMENDED_DOWNGRADES_VIEW_BUILDER.build_and_print()
