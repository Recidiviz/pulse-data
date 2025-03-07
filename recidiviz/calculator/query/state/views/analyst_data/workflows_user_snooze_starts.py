# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View with events where the officer snoozed a client, according to export archive.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_user_snooze_starts"

_VIEW_DESCRIPTION = (
    "Events where the officer snoozed a client, according to export archive"
)

_QUERY_TEMPLATE = """
SELECT
    CASE state_code WHEN "US_ID" THEN "US_IX" ELSE state_code END AS state_code, 
    LOWER(snoozed_by) AS email_address,
    opportunity_type,
    snooze_start_date,
    person_external_id,
FROM
    `{project_id}.export_archives.workflows_snooze_status_archive`
"""

WORKFLOWS_USER_SNOOZE_STARTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    clustering_fields=["state_code", "email_address"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USER_SNOOZE_STARTS_VIEW_BUILDER.build_and_print()
