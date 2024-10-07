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

"""A view revealing opportunity types with no rows in the `person_record` view."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OPPORTUNITIES_WITHOUT_PERSON_RECORDS_VIEW_NAME = "opportunites_without_person_records"

OPPORTUNITIES_WITHOUT_PERSON_RECORDS_DESCRIPTION = (
    """Opportunity types that do not have any clients/residents in `person_record`"""
)

OPPORTUNITIES_WITHOUT_PERSON_RECORDS_QUERY_TEMPLATE = """
WITH unnested_records AS (
  SELECT * except (all_eligible_opportunities)
  FROM `{project_id}.{workflows_dataset}.person_record_materialized` records, UNNEST(all_eligible_opportunities) AS opportunity_type
)

SELECT
  state_code, state_code AS region_code, opportunity_type
FROM `{project_id}.{reference_views_dataset}.workflows_opportunity_configs_materialized`
LEFT JOIN unnested_records USING (state_code, opportunity_type)
WHERE unnested_records.person_external_id IS NULL
"""

OPPORTUNITIES_WITHOUT_PERSON_RECORDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OPPORTUNITIES_WITHOUT_PERSON_RECORDS_VIEW_NAME,
    view_query_template=OPPORTUNITIES_WITHOUT_PERSON_RECORDS_QUERY_TEMPLATE,
    description=OPPORTUNITIES_WITHOUT_PERSON_RECORDS_DESCRIPTION,
    reference_views_dataset=state_dataset_config.REFERENCE_VIEWS_DATASET,
    workflows_dataset=state_dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OPPORTUNITIES_WITHOUT_PERSON_RECORDS_VIEW_BUILDER.build_and_print()
