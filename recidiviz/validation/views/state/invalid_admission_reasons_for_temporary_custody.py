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
"""A view revealing when admission metrics have an admission_reason other than
TEMPORARY_CUSTODY for periods of temporary custody (either PAROLE_BOARD_HOLD or
TEMPORARY_CUSTODY specialized_purpose_for_incarceration values).

Existence of any rows indicates a bug in IP pre-processing logic.
"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_NAME = (
    "invalid_admission_reasons_for_temporary_custody"
)

INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_DESCRIPTION = """ Incarceration admission metrics with invalid admission reasons for periods of
    temporary custody."""

INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT *
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics_materialized`
    -- All admissions with PAROLE_BOARD_HOLD or TEMPORARY_CUSTODY pfi values should 
    -- have a TEMPORARY_CUSTODY admission reason
    WHERE specialized_purpose_for_incarceration IN ('PAROLE_BOARD_HOLD', 'TEMPORARY_CUSTODY')
    AND admission_reason != 'TEMPORARY_CUSTODY'
"""

INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_NAME,
    view_query_template=INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_QUERY_TEMPLATE,
    description=INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_DESCRIPTION,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER.build_and_print()
