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
"""Preprocessed view showing spans of time when a person had a permanent exemption from fines/fees in TN, and
also capturing all exemption reasons during a given span."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_NAME = (
    "us_tn_permanent_exemptions_preprocessed"
)

US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view showing spans of time when a person had
a permanent exemption from fines/fees in TN, and also capturing all exemption reasons during a given span."""

US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        external_id,
        start_date,
        end_date AS end_date_exclusive,
        TRUE AS has_permanent_exemption,
        ARRAY_AGG(DISTINCT ReasonCode ORDER BY ReasonCode) AS permanent_exemption_reasons
    FROM
        `{project_id}.{analyst_dataset}.us_tn_exemptions_preprocessed`
    WHERE
        ReasonCode IN ("SSDB", "JORD", "CORD", "SISS")
    GROUP BY
        1,2,3,4,5,6

"""

US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
