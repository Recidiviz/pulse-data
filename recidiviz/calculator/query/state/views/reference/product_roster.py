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
"""View of all users that may have access to Polaris products"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.views.dataset_config import CASE_TRIAGE_FEDERATED_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRODUCT_ROSTER_VIEW_NAME = "product_roster"

PRODUCT_ROSTER_DESCRIPTION = """******NOT READY YET, DO NOT USE.******
View of all users that may have access to Polaris products. Pulls data from roster Cloud SQL tables.
Should only be used for Polaris product-related views."""

PRODUCT_ROSTER_QUERY_TEMPLATE = """
    SELECT
        {columns_query}
    FROM
        `{project_id}.{case_triage_federated_dataset_id}.roster` roster
    FULL OUTER JOIN
        `{project_id}.{case_triage_federated_dataset_id}.user_override` override
    USING (email_address)
"""

PRODUCT_ROSTER_COLUMNS = [
    "state_code",
    "external_id",
    "email_address",
    "role",
    "district",
]

PRODUCT_ROSTER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    case_triage_federated_dataset_id=CASE_TRIAGE_FEDERATED_DATASET,
    view_id=PRODUCT_ROSTER_VIEW_NAME,
    view_query_template=PRODUCT_ROSTER_QUERY_TEMPLATE,
    description=PRODUCT_ROSTER_DESCRIPTION,
    should_materialize=True,
    columns_query="\n        ".join(
        [
            f"COALESCE(override.{col}, roster.{col}) AS {col},"
            for col in PRODUCT_ROSTER_COLUMNS
        ]
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_ROSTER_VIEW_BUILDER.build_and_print()
