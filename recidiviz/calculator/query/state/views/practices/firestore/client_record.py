#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View to prepare client records regarding compliant reporting for export to the frontend."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_RECORD_VIEW_NAME = "client_record"

CLIENT_RECORD_DESCRIPTION = """
    Nested client records to be exported to Firestore to power the Compliant Reporting dashboard in TN.
    """

CLIENT_RECORD_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        person_external_id,
        "US_TN" AS state_code,
        person_name,
        officer_id,
        supervision_type,
        supervision_level,
        supervision_level_start,
        address,
        phone_number,
        expiration_date,
        current_balance,
        last_payment_amount,
        last_payment_date,
        exemption_notes AS fee_exemptions,
        special_conditions_on_current_sentences AS special_conditions,
        SPE_note_due AS next_special_conditions_check,
        compliant_reporting_eligible IS NOT NULL AND compliant_reporting_eligible = 'c1' AS eligible,
        compliant_reporting_eligible IS NOT NULL AND compliant_reporting_eligible IN ('c1', 'c2', 'c3') AS eligible_with_discretion,
        current_offenses AS offense_type,
        judicial_district,
        ARRAY(SELECT DRUN.contact_date FROM UNNEST(last_DRUN) AS DRUN) AS last_drug_negative,
        last_sanction,
    FROM `{project_id}.{analyst_views_dataset}.us_tn_compliant_reporting_logic_materialized`
"""

CLIENT_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PRACTICES_VIEWS_DATASET,
    view_id=CLIENT_RECORD_VIEW_NAME,
    view_query_template=CLIENT_RECORD_QUERY_TEMPLATE,
    description=CLIENT_RECORD_DESCRIPTION,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_VIEW_BUILDER.build_and_print()
