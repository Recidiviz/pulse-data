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
"""Reference table with metadata about specific
locations in the state that can be associated with a person or staff member."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LOCATION_METADATA_VIEW_NAME = "location_metadata"

LOCATION_METADATA_DESCRIPTION = """Reference table with metadata about specific
locations in the state that can be associated with a person or staff member. 

The `location_external_id` values in this table should encompass any external ids that
the *state* uses to describe a person or staff member's location. This ID will generally
correspond to the smallest level of location granularity available and the
`location_metadata` will describe where that location stands in a hierarchy of 
locations.

For example, in some states the location_external_ids might correspond to a supervision
unit with metadata fields describing the office/district/region associated with that 
unit. In that state there might not be any rows specifically dedicated to a full
district or region. We would get a list of districts by selecting all
`supervision_district` values in the location_metadata field.

## Field Definitions

| Field | Description |
| ------- | ----------- |
| state_code | State |
| location_external_id | The state-issued stable id associated with this location. This must match the `location_external_id` format on the `state_staff_location_period` table. |
| location_name | A human-readable name for this location. This may be used as a display name in products and should generally not include acronyms. |
| location_type | Describes the type of location. Values must be one of the values defined in the StateLocationType enum. |
| location_metadata | JSON-formatted additional metadata about this location. You may only use the keys specified below. |

## `location_metadata` allowed dictionary keys
The `location_metadata` JSON may have any of the following keys, but should not have keys outside this list.

When filling out the `supervision_*` fields in `location_metadata`, please hydrate according to the following definitions:
- Supervision unit: A group of officers associated with a common manager officer.
- Supervision office: A place/building where officers may show up to work, meet clients, conduct trainings, etc. Usually has a physical address.
- Supervision district: A a grouping of offices at the finest level available. The DOC may have employees associated with this level responsible for outcomes across multiple supervision units and/or offices. Usually has a defined geographic boundary composed of one or several counties.
- Supervision region: A grouping of offices and/or districts at one level coarser than district.


| Key | Description |
| ------- | ----------- |
| county_id | The external id for the county this location is in. |
| county_name | The human-readable name for the county this location is in. |
| is_active_location | A flag indicating whether this location is currently active. |
| location_subtype | Extra location type information not provided by location_type. |
| location_acronym | An acronym / abbreviation used to reference a given location (often a facility). For example, SMP to represent 'State Prison of Southern Michigan'. |
| facility_group_external_id |  A grouping for facility locations that may be provided by the state and is useful for analysis (e.g. groups all out-of-state facilities together). |
| facility_group_name |  The human-readable name for this location's facility group. |
| facility_security_level | The security level associated with this location, if it is a facility (e.g. Minimum Security). |
| supervision_unit_id | The external id for this location's supervision unit (see definition above). |
| supervision_unit_name | The human-readable name for this location's supervision unit (see definition above). |
| supervision_office_id | The external id for this location's supervision office (see definition above). |
| supervision_office_name | The human-readable name for this location's supervision office (see definition above). |
| supervision_district_id | The external id for this location's supervision district (see definition above). |
| supervision_district_name |   The human-readable name for this location's supervision district (see definition above). |
| supervision_region_id | The external id for this location's supervision region (see definition above). |
| supervision_region_name |   The human-readable name for this location's supervision region (see definition above). |
"""

# TODO(#19319): Hydrate US_ME location metadata in this view
# TODO(#19316): Hydrate US_MI location metadata in this view
# TODO(#19317): Hydrate US_IX location metadata in this view
LOCATION_METADATA_QUERY_TEMPLATE = """
SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_ix_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_nd_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_pa_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_ca_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_tn_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_mi_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_ar_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_az_location_metadata_materialized`

UNION ALL

SELECT state_code, location_external_id, location_name, location_type, location_metadata
FROM `{project_id}.{reference_views_dataset}.us_or_location_metadata_materialized`
"""

LOCATION_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=LOCATION_METADATA_VIEW_NAME,
    view_query_template=LOCATION_METADATA_QUERY_TEMPLATE,
    description=LOCATION_METADATA_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_METADATA_VIEW_BUILDER.build_and_print()
