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
"""Creates the view builder and view for officer attributes at the time of variant
assignment for those in an experiment."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFICER_ATTRIBUTES_VIEW_NAME = "officer_attributes"

OFFICER_ATTRIBUTES_VIEW_DESCRIPTION = (
    "Calculates officer-level attributes for those assigned a variant in the "
    "assignments table. All attributes are specific to officer-experiment-variant, so "
    "there may be multiple observations per officer and multiple observations per "
    "officer-experiment. All attributes are calculated at time of variant assignment."
)

OFFICER_ATTRIBUTES_PRIMARY_KEYS = (
    "experiment_id, state_code, officer_external_id, variant_date"
)

OFFICER_ATTRIBUTES_QUERY_TEMPLATE = """
WITH participants AS (
    SELECT DISTINCT
        {primary_keys},
    FROM
        `{project_id}.{experiments_dataset}.officer_assignments_materialized`
)

# PO district at time of variant assignment
, po_districts AS (
    SELECT DISTINCT
        experiment_id,
        a.state_code,
        officer_external_id,
        variant_date,
        "DISTRICT" AS attribute,
        IFNULL(district, "INTERNAL_UNKNOWN") AS value,
    FROM
        participants a
    LEFT JOIN
        `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` b
    ON
        a.state_code = b.state_code
        AND a.officer_external_id = b.officer_external_id
        AND EXTRACT(MONTH from a.variant_date) = b.month
        AND EXTRACT(YEAR from a.variant_date) = b.year
)

# union CTEs in long format
SELECT * FROM po_districts
"""

OFFICER_ATTRIBUTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=OFFICER_ATTRIBUTES_VIEW_NAME,
    view_query_template=OFFICER_ATTRIBUTES_QUERY_TEMPLATE,
    description=OFFICER_ATTRIBUTES_VIEW_DESCRIPTION,
    experiments_dataset=EXPERIMENTS_DATASET,
    po_report_dataset=PO_REPORT_DATASET,
    primary_keys=OFFICER_ATTRIBUTES_PRIMARY_KEYS,
    should_materialize=True,
    clustering_fields=["experiment_id", "attribute"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_ATTRIBUTES_VIEW_BUILDER.build_and_print()
