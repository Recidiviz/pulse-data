# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
This tracks the assignment of treatment variants to states in an experiment. Data is 
sourced from two locations: a Google Sheet-backed table, typically used for experiments 
with fewer treatment variants, and a manually updated table, generally used for 
experiments with a larger number of variants."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments_metadata.dataset_config import (
    EXPERIMENTS_METADATA_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EXPERIMENT_ASSIGNMENTS_VIEW_NAME = "experiment_assignments"

EXPERIMENT_ASSIGNMENTS_VIEW_DESCRIPTION = """
This tracks the assignment of treatment variants to units in an experiment. Data is 
sourced from two locations: a Google Sheet-backed table, typically used for experiments 
with fewer treatment variants, and a manually updated table, generally used for 
experiments with a larger number of variants."""

EXPERIMENT_ASSIGNMENTS_QUERY_TEMPLATE = """
SELECT 
  state_code,
  experiment_id,
  unit_id,
  unit_type,
  variant_id,
  variant_date,
FROM `{project_id}.google_sheet_backed_tables.experiment_assignments_sheet`

UNION ALL

SELECT 
  state_code,
  experiment_id,
  unit_id,
  unit_type,
  variant_id,
  variant_date,
FROM `{project_id}.manually_updated_source_tables.experiment_assignments_large`
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, experiment_id, unit_id, variant_id, unit_type, variant_date ORDER BY upload_datetime DESC) = 1
"""

EXPERIMENT_ASSIGNMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_METADATA_DATASET,
    view_id=EXPERIMENT_ASSIGNMENTS_VIEW_NAME,
    view_query_template=EXPERIMENT_ASSIGNMENTS_QUERY_TEMPLATE,
    description=EXPERIMENT_ASSIGNMENTS_VIEW_DESCRIPTION,
    should_materialize=True,
    clustering_fields=["experiment_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EXPERIMENT_ASSIGNMENTS_VIEW_BUILDER.build_and_print()
