# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Arizona resident metadata"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_RESIDENT_METADATA_VIEW_NAME = "us_az_resident_metadata"

US_AZ_RESIDENT_METADATA_VIEW_DESCRIPTION = """
Arizona resident metadata
"""
US_AZ_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = """
    WITH all_residents AS (
        SELECT
            state_code,
            person_id,
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
        WHERE state_code = 'US_AZ'
        AND compartment_level_1 = 'INCARCERATION'
        AND end_date_exclusive IS NULL
    ),
    all_sed_dates AS (
        SELECT
            person_id,
            state_code,
            projected_full_term_release_date_max as sed_date,
        FROM all_residents
        LEFT JOIN `{project_id}.{us_az_normalized_state_dataset}.state_sentence_group`
        USING (state_code, person_id)
        LEFT JOIN `{project_id}.{sentence_sessions_dataset}.sentence_inferred_group_projected_dates_materialized`
        USING (state_code, person_id, sentence_inferred_group_id)
        QUALIFY ROW_NUMBER()
            OVER (PARTITION BY person_id, state_code, sentence_inferred_group_id ORDER BY inferred_group_update_datetime DESC) = 1
    ), sed_dates AS (
        SELECT
            state_code,
            person_id,
            -- TODO(#34661): Correctly select active sentence
            MAX(sed_date) AS sed_date,
        FROM all_sed_dates
        GROUP BY 1, 2
    )

    SELECT
        state_code,
        person_id,
        sed_date,
        ercd_date,
        csbd_date,
        projected_csbd_date,
        acis_tpr_date,
        projected_tpr_date,
        acis_dtp_date,
        projected_dtp_date,
    FROM sed_dates
    LEFT JOIN `{project_id}.{analyst_data_dataset}.us_az_projected_dates_materialized`
    USING (state_code, person_id)
    WHERE end_date IS NULL
    """

US_AZ_RESIDENT_METADATA_VIEW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_AZ_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_AZ_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_AZ_RESIDENT_METADATA_VIEW_DESCRIPTION,
    analyst_data_dataset="analyst_data",
    us_az_normalized_state_dataset="us_az_normalized_state",
    sessions_dataset="sessions",
    sentence_sessions_dataset="sentence_sessions",
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_RESIDENT_METADATA_VIEW_VIEW_BUILDER.build_and_print()
