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
"""Constants related to source table datasets that contain tables whose schemas are
managed by our standard source table update process, with schemas defined in a YAML file
in this directory.
"""
from recidiviz.calculator.query.state.dataset_config import (
    POPULATION_PROJECTION_OUTPUT_DATASET,
    SPARK_OUTPUT_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)

# Views that are updated manually
MANUALLY_UPDATED_SOURCE_TABLES_DATASET: str = "manually_updated_source_tables"

# Views backed by Google Sheets
GOOGLE_SHEET_BACKED_TABLES_DATASET: str = "google_sheet_backed_tables"

VERA_DATASET: str = "vera_data"

VIEW_UPDATE_METADATA_DATASET: str = "view_update_metadata"

YAML_MANAGED_DATASETS_TO_DESCRIPTIONS = {
    GOOGLE_SHEET_BACKED_TABLES_DATASET: (
        "Stores views that are backed by Google Sheets."
    ),
    MANUALLY_UPDATED_SOURCE_TABLES_DATASET: (
        "Stores source tables that are updated manually."
    ),
    POPULATION_PROJECTION_OUTPUT_DATASET: (
        "Stores output of the population projection simulations."
    ),
    SPARK_OUTPUT_DATASET: "Stores output of Spark simulations",
    STATIC_REFERENCE_TABLES_DATASET: (
        "Reference tables used by various views in BigQuery. May need to be updated manually for new states."
    ),
    VERA_DATASET: (
        "Stores data calculated outside of our codebase by Vera. Used only by Vera."
    ),
    VIEW_UPDATE_METADATA_DATASET: (
        "Stores metadata about the performance of our view update process"
    ),
}
