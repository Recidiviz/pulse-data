# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
This validation view compares sentencing.case_insights_rates with
sentencing_views.sentencing_charge_record_materialized to identify charges that
are in charge_record but not in case_insights_rates
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SENTENCING_CASE_INSIGHTS_RATES_MISSING_CHARGES_VIEW_NAME = (
    "case_insights_rates_missing_charges"
)

SENTENCING_CASE_INSIGHTS_RATES_MISSING_CHARGES_DESCRIPTION = """
Shows charges in charge_record_materialized that are not in 
sentencing.case_insights_rates
"""

MISSING_CHARGES = """
    SELECT DISTINCT 
        sc.state_code AS state_code,
        sc.state_code AS region_code, 
        sc.description
    FROM 
        `{project_id}.normalized_state.state_charge_v2`  sc
    LEFT JOIN
        `{project_id}.sentencing.case_insights_rates` cir
    ON 
        sc.state_code = cir.state_code 
        AND sc.description = cir.most_severe_description
    WHERE 
        cir.most_severe_description IS NULL 
"""

SENTENCES_CASE_INSIGHTS_CHARGE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SENTENCING_CASE_INSIGHTS_RATES_MISSING_CHARGES_VIEW_NAME,
    view_query_template=MISSING_CHARGES,
    description=SENTENCING_CASE_INSIGHTS_RATES_MISSING_CHARGES_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_CASE_INSIGHTS_CHARGE_COMPARISON_VIEW_BUILDER.build_and_print()
