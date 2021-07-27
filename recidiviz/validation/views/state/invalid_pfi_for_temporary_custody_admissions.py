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
"""A view revealing when admission metrics have a specialized_purpose_for_incarceration
other than TEMPORARY_CUSTODY or PAROLE_BOARD_HOLD for admissions with an
admission_reason of TEMPORARY_CUSTODY.

Existence of any rows indicates a bug in IP pre-processing logic.
"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_NAME = (
    "invalid_pfi_for_temporary_custody_admissions"
)

INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_DESCRIPTION = """Incarceration admission metrics with invalid pfi values for TEMPORARY_CUSTODY admissions."""

INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT state_code AS region_code,
    * EXCEPT (state_code)
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics_materialized`
    -- Only PAROLE_BOARD_HOLD and TEMPORARY_CUSTODY periods should have an 
    -- admission_reason of TEMPORARY_CUSTODY
    WHERE admission_reason = 'TEMPORARY_CUSTODY'
    AND specialized_purpose_for_incarceration NOT IN ('PAROLE_BOARD_HOLD', 'TEMPORARY_CUSTODY')
"""

INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_NAME,
    view_query_template=INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_QUERY_TEMPLATE,
    description=INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_DESCRIPTION,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_BUILDER.build_and_print()
