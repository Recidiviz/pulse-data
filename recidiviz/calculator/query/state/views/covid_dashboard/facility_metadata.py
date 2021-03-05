# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Facility names, IDs, and additional metadata for all facilities for which we have case data."""
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FACILITY_METADATA_VIEW_NAME = "facility_metadata"

FACILITY_METADATA_VIEW_DESCRIPTION = """Facility names, IDs, and additional metadata for all facilities for which we have case data."""

FACILITY_METADATA_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    -- This ensures we are using the primary name from the alias table
    primary_alias AS (
      SELECT * FROM `{project_id}.{covid_dashboard_reference_dataset}.facility_alias`
      WHERE primary_name IS TRUE
    ),
    -- This table has the least sparse county data
    locations AS (
      SELECT
        facility_id, l.*
      FROM `{project_id}.{covid_dashboard_reference_dataset}.facility_locations` l
      INNER JOIN `{project_id}.{covid_dashboard_reference_dataset}.facility_alias` 
        USING (state, facility_name)
    ),
    -- This has most of the other metadata
    attributes AS (
      -- A facility can appear in the facility_attributes table more than once under different names,
      -- but are properly connected to a single facility ID.
      -- We want any non-null value for each attribute, for each distinct facility ID.
      SELECT DISTINCT
        facility_id,
        FIRST_VALUE(capacity IGNORE NULLS) OVER facility_window as capacity,
        FIRST_VALUE(population_year_updated IGNORE NULLS) OVER facility_window as population_year_updated,
        FIRST_VALUE(population IGNORE NULLS) OVER facility_window as population
      FROM `{project_id}.{covid_dashboard_reference_dataset}.facility_attributes` a
      INNER JOIN `{project_id}.{covid_dashboard_reference_dataset}.facility_alias` 
        USING (state, facility_name)
      WINDOW facility_window AS (PARTITION BY facility_id ORDER BY population_year_updated DESC)
    )
    
    SELECT DISTINCT
      facility_id,
      primary_alias.facility_name,
      primary_alias.facility_type,
      -- The locations table is more reliable for counties
      locations.county,
      -- The alias table is more reliable for states
      primary_alias.state,
      capacity,
      population_year_updated,
      population
    FROM
      `{project_id}.{covid_dashboard_dataset}.facility_case_data`
    LEFT JOIN
      primary_alias
    USING (facility_id)
    LEFT JOIN
      locations
    USING (facility_id)
    LEFT JOIN
      attributes
    USING (facility_id)
    ORDER BY facility_id
    """

FACILITY_METADATA_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_DASHBOARD_DATASET,
    view_id=FACILITY_METADATA_VIEW_NAME,
    view_query_template=FACILITY_METADATA_VIEW_QUERY_TEMPLATE,
    description=FACILITY_METADATA_VIEW_DESCRIPTION,
    dimensions=("facility_id",),
    covid_dashboard_dataset=dataset_config.COVID_DASHBOARD_DATASET,
    covid_dashboard_reference_dataset=dataset_config.COVID_DASHBOARD_REFERENCE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FACILITY_METADATA_VIEW_BUILDER.build_and_print()
