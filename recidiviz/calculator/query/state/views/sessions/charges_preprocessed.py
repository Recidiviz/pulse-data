# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Processed Sentencing Data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGES_PREPROCESSED_VIEW_NAME = "charges_preprocessed"

CHARGES_PREPROCESSED_VIEW_DESCRIPTION = """Processed Charge Data"""

# List of states that have separate sentence preprocessed views
CHARGES_PREPROCESSED_SPECIAL_STATES = ["US_MO"]

CHARGES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        charge.*,
        COALESCE(court.judicial_district_code, 'EXTERNAL_UNKNOWN') AS judicial_district,
    FROM `{project_id}.{analyst_dataset}.state_charge_with_labels_materialized` charge
    -- Pull the judicial district from state_court_case
    LEFT JOIN `{project_id}.{state_base_dataset}.state_court_case` court
        ON court.state_code = charge.state_code
        AND court.court_case_id = charge.court_case_id
    WHERE charge.state_code NOT IN ('{special_states}')

    UNION ALL

    SELECT
        *
    FROM `{project_id}.{sessions_dataset}.us_mo_charges_preprocessed`
"""

CHARGES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CHARGES_PREPROCESSED_VIEW_NAME,
    view_query_template=CHARGES_PREPROCESSED_QUERY_TEMPLATE,
    description=CHARGES_PREPROCESSED_VIEW_DESCRIPTION,
    state_base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    special_states="', '".join(CHARGES_PREPROCESSED_SPECIAL_STATES),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGES_PREPROCESSED_VIEW_BUILDER.build_and_print()
