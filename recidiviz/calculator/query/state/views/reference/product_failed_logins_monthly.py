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
"""View of all users that have failed logins on dashboards within the last month."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_NAME = "product_failed_logins_monthly"

PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_DESCRIPTION = (
    """View of all users that have failed logins on dashboards within the last month."""
)

PRODUCT_FAILED_LOGINS_MONTHLY_QUERY_TEMPLATE = """
    SELECT email, state_code, original_timestamp
    FROM `{auth0_project_name}.auth0_prod_action_logs.failed_login`
    WHERE DATE(original_timestamp) >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)
    ORDER BY original_timestamp DESC
"""

PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_NAME,
    view_query_template=PRODUCT_FAILED_LOGINS_MONTHLY_QUERY_TEMPLATE,
    description=PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_DESCRIPTION,
    # The auth_prod_action dataset only exists in production, so hard coding the project here.
    # This means that the view and export in staging will also have production data
    auth0_project_name=GCP_PROJECT_PRODUCTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER.build_and_print()
