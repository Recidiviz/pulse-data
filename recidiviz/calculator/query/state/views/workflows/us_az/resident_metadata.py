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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_RESIDENT_METADATA_VIEW_NAME = "us_az_resident_metadata"

US_AZ_RESIDENT_METADATA_VIEW_DESCRIPTION = """
Arizona resident metadata
"""
US_AZ_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = f"""
    WITH all_residents AS (
        SELECT
            state_code,
            person_id,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
        WHERE state_code = 'US_AZ'
        AND compartment_level_1 = 'INCARCERATION'
        AND end_date_exclusive IS NULL
    ), projected_tpr_visible_in_tool AS (
        SELECT
            state_code,
            person_id,
        FROM
            `{{project_id}}.task_eligibility_spans_us_az.overdue_for_recidiviz_tpr_request_materialized`
        WHERE
            (is_eligible OR is_almost_eligible)
            AND CURRENT_DATE("US/Eastern") BETWEEN start_date AND {nonnull_end_date_exclusive_clause("end_date")}
    ), projected_dtp_visible_in_tool AS (
        SELECT
            state_code,
            person_id,
        FROM
            `{{project_id}}.task_eligibility_spans_us_az.overdue_for_recidiviz_dtp_request_materialized`
        WHERE
            (is_eligible OR is_almost_eligible)
            AND CURRENT_DATE("US/Eastern") BETWEEN start_date AND {nonnull_end_date_exclusive_clause("end_date")}
    )

    SELECT
        state_code,
        person_id,
        sed_date,
        ercd_date,
        csbd_date,
        projected_csbd_date,
        acis_tpr_date,
        IF(
            projected_tpr_visible_in_tool.person_id IS NOT NULL
                AND acis_tpr_date IS NULL,
            projected_tpr_date,
            NULL
        ) AS projected_tpr_date,
        acis_dtp_date,
        IF(
            projected_dtp_visible_in_tool.person_id IS NOT NULL
                AND acis_dtp_date IS NULL,
            projected_dtp_date,
            NULL
        ) AS projected_dtp_date,
        csed_date,
    FROM all_residents
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_az_projected_dates_materialized` projected_dates
    USING (state_code, person_id)
    LEFT JOIN projected_tpr_visible_in_tool
    USING (state_code, person_id)
    LEFT JOIN projected_dtp_visible_in_tool
    USING (state_code, person_id)
    -- Use the sentence dates from the latest projected date span to include residents
    -- who are incarcerated but do not have an active sentence
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id
        ORDER BY projected_dates.start_date DESC, projected_dates.end_date DESC
    ) = 1
    """

US_AZ_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_AZ_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_AZ_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_AZ_RESIDENT_METADATA_VIEW_DESCRIPTION,
    analyst_data_dataset="analyst_data",
    sessions_dataset="sessions",
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
