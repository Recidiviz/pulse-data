# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view revealing when monthly district/state averages are not equal across all records in the district/state."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_NAME = 'po_report_avgs_per_district_state'

PO_REPORT_AVGS_PER_DISTRICT_STATE_DESCRIPTION = """
  Monthly supervision metrics averaged by district and state that do not match across the district and/or state.
"""

PO_REPORT_AVGS_PER_DISTRICT_STATE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'pos_discharges_district_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.pos_discharges_district_average != t2.pos_discharges_district_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'supervision_downgrades_district_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.supervision_downgrades_district_average != t2.supervision_downgrades_district_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'earned_discharges_district_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.earned_discharges_district_average != t2.earned_discharges_district_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'technical_revocations_district_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.technical_revocations_district_average != t2.technical_revocations_district_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'crime_revocations_district_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.crime_revocations_district_average != t2.crime_revocations_district_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'absconsions_district_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.absconsions_district_average != t2.absconsions_district_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'pos_discharges_state_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month)
    WHERE t1.pos_discharges_state_average != t2.pos_discharges_state_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'supervision_downgrades_state_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month, district)
    WHERE t1.supervision_downgrades_state_average != t2.supervision_downgrades_state_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'earned_discharges_state_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month)
    WHERE t1.earned_discharges_state_average != t2.earned_discharges_state_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'technical_revocations_state_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month)
    WHERE t1.technical_revocations_state_average != t2.technical_revocations_state_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'crime_revocations_state_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month)
    WHERE t1.crime_revocations_state_average != t2.crime_revocations_state_average

    UNION ALL

    SELECT DISTINCT
      state_code as region_code, review_month, t1.email_address, 'absconsions_state_average-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    LEFT JOIN `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t2
      USING (state_code, review_month)
    WHERE t1.absconsions_state_average != t2.absconsions_state_average
    """

PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_NAME,
    view_query_template=PO_REPORT_AVGS_PER_DISTRICT_STATE_QUERY_TEMPLATE,
    description=PO_REPORT_AVGS_PER_DISTRICT_STATE_DESCRIPTION,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_BUILDER.build_and_print()
