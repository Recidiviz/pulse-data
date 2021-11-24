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
"""A view revealing when we see invalid combinations of admission_reason and
specialized_purpose_for_incarceration.
Existence of any rows indicates a bug in IP pre-processing logic.
"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INVALID_ADMISSION_REASON_AND_PFI_VIEW_NAME = "invalid_admission_reason_and_pfi"

INVALID_ADMISSION_REASON_AND_PFI_DESCRIPTION = """Incarceration admission metrics with invalid combinations of admission_reason and specialized_purpose_for_incarceration."""

INVALID_ADMISSION_REASON_AND_PFI_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT state_code AS region_code,
    * EXCEPT (state_code)
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics_included_in_state_population_materialized`
    -- Any REVOCATION admission_reason should have a purpose of GENERAL
    -- TODO(#9866): Change to '=REVOCATION' once the admission reason enum is REVOCATION.
    WHERE (admission_reason LIKE '%REVOCATION'
        AND specialized_purpose_for_incarceration NOT IN ('GENERAL'))
    -- Any NEW_ADMISSION should have a purpose of GENERAL, TREATMENT_IN_PRISON, INTERNAL_UNKNOWN, or SHOCK_INCARCERATION
    OR (admission_reason = 'NEW_ADMISSION'
        AND specialized_purpose_for_incarceration NOT IN ('GENERAL', 'TREATMENT_IN_PRISON', 'INTERNAL_UNKNOWN', 'SHOCK_INCARCERATION'))
    -- ANY SANCTION_ADMISSION should have a purpose of TREATMENT_IN_PRISON or SHOCK_INCARCERATION
    OR (admission_reason = 'SANCTION_ADMISSION'
        AND specialized_purpose_for_incarceration NOT IN ('TREATMENT_IN_PRISON', 'SHOCK_INCARCERATION'))
"""

INVALID_ADMISSION_REASON_AND_PFI_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_ADMISSION_REASON_AND_PFI_VIEW_NAME,
    view_query_template=INVALID_ADMISSION_REASON_AND_PFI_QUERY_TEMPLATE,
    description=INVALID_ADMISSION_REASON_AND_PFI_DESCRIPTION,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_ADMISSION_REASON_AND_PFI_VIEW_BUILDER.build_and_print()
