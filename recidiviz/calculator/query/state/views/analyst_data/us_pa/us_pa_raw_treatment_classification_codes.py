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
"""Classification Codes Mapped to Treatment Type in US_PA"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_NAME = (
    "us_pa_raw_treatment_classification_codes"
)

US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_DESCRIPTION = (
    """Classification Codes Mapped to Treatment Type in US_PA"""
)

US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_QUERY_TEMPLATE = """
    #TODO(#14932): Deprecate view when we have "ingested" table for classification codes
    SELECT
        SPLIT(TrtClassCode,'-')[OFFSET(0)] AS classification_code,
        SPLIT(TrtClassCode,'-')[OFFSET(1)] AS classification_description,
    FROM UNNEST([
        '10-Alcohol/Drug',
        '50-Assistance',
        '110-BCC Referrals',
        '99-Community Service Programs',
        '100-Day Reporting',
        '20-Domestic Relations',
        '30-Education',
        '40-Employment Services',
        '120-Gambling',
        '93-Halfway Houses',
        '70-Health Services',
        '60-Housing',
        '80-Human Relations',
        '92-Legal Programs',
        '90-Mental Health',
        '88-Parole Violation',
        '85-PVCCC',
        '91-Veterans Programs'
    ]) AS TrtClassCode
    """

US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_NAME,
    view_query_template=US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_QUERY_TEMPLATE,
    description=US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_BUILDER.build_and_print()
