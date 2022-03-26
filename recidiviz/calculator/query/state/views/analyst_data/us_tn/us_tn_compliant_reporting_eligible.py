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
"""Creates a view that surfaces people eligible for Compliant Reporting"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_NAME = "us_tn_compliant_reporting_eligible"

US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_DESCRIPTION = (
    """Creates a view that surfaces people eligible for Compliant Reporting"""
)

US_TN_COMPLIANT_REPORTING_ELIGIBLE_QUERY_TEMPLATE = """
    SELECT
        person_name,
        person_external_id,
        address,
        phone_number,
        officer_id,
        supervision_type,
        expiration_date,
        judicial_district,
        supervision_level,
        eligible_level_start,
        supervision_level_start,
        latest_supervision_start_date,
        SPE_note_due,
        current_offenses,
        lifetime_offenses_expired,
        prior_offenses,
        last_DRUN,
        sanctions_in_last_year,
        last_arr_check_past_year,
        Last_ARR_Note AS date_last_arr_check_past_year,
        current_balance,
        last_payment_date,
        last_payment_amount,
        exemption_notes,
        special_conditions_on_current_sentences,
        greatest_date_eligible,
        compliant_reporting_eligible,
        district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_logic_materialized`
"""

US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_ELIGIBLE_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    project_id=GCP_PROJECT_STAGING,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_BUILDER.build_and_print()
