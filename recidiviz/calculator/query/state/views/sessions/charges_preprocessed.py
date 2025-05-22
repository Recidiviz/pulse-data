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
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGES_PREPROCESSED_VIEW_NAME = "charges_preprocessed"

CHARGES_PREPROCESSED_VIEW_DESCRIPTION = """Processed Charge Data"""

CHARGES_PREPROCESSED_QUERY_TEMPLATE = """
    WITH charge AS (
        SELECT
            charge_id,
            NULL AS charge_v2_id,
            * EXCEPT (charge_id),
        FROM `{project_id}.{normalized_state_dataset}.state_charge`
        WHERE state_code IN ({v2_non_migrated_states})

        UNION ALL

        SELECT
            NULL AS charge_id,
            charge_v2_id,
            * EXCEPT (charge_v2_id),
        FROM `{project_id}.{normalized_state_dataset}.state_charge_v2`
        WHERE state_code NOT IN ({v2_non_migrated_states})
    )
    SELECT
        charge.*,
        charge_labels.* EXCEPT(offense_description, probability),
        COALESCE(
            charge.judicial_district_code,
            TRIM(scc.judicial_district_code),
            'EXTERNAL_UNKNOWN'
        ) AS judicial_district,
    FROM charge
    LEFT JOIN `{project_id}.{static_reference_dataset}.state_county_codes` scc
    USING (state_code, county_code)
    LEFT JOIN `{project_id}.reference_views.cleaned_offense_description_to_labels` charge_labels
    ON charge.description = charge_labels.offense_description
"""

CHARGES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CHARGES_PREPROCESSED_VIEW_NAME,
    view_query_template=CHARGES_PREPROCESSED_QUERY_TEMPLATE,
    description=CHARGES_PREPROCESSED_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGES_PREPROCESSED_VIEW_BUILDER.build_and_print()
