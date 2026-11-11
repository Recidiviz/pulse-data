# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Helper SQL queries and criteria view builders for Idaho facilities, including
Placement Matrix facilities, Community Reentry Centers (CRCs), and stays with a
given incarceration purpose."""

from enum import Enum

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_release_district_spans import (
    UsIxReleaseDistrict,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.observations.span_observation_big_query_view_builder import fix_indent
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)


class UsIxPlacementMatrixFacility(Enum):
    """Facilities on page 2 of the IDOC Placement Matrix
    spelled to match housing_unit_sessions.facility entries.
    """

    ICIO = "IDAHO CORRECTIONAL INSTITUTION-OROFINO"
    ISCC = "IDAHO STATE CORRECTIONAL CENTER"
    ISCI = "IDAHO STATE CORRECTIONAL INSTITUTION"
    # CAPP was renamed to Mountain View Transformation Center
    MVTC = "MOUNTAIN VIEW TRANSFORMATION CENTER"
    NICI = "NORTH IDAHO CORRECTIONAL INSTITUTION"

    @property
    def reception_housing_unit_prefix(self) -> str | None:
        """Returns the start of the housing unit names in the facility's Reception
        and Diagnostic Unit (RDU), such as "UNIT 15-" for ISCI, or None if the
        facility has no RDU."""
        if self is UsIxPlacementMatrixFacility.ISCI:
            return "UNIT 15-"
        if self in (
            UsIxPlacementMatrixFacility.ICIO,
            UsIxPlacementMatrixFacility.ISCC,
            UsIxPlacementMatrixFacility.MVTC,
            UsIxPlacementMatrixFacility.NICI,
        ):
            return None
        raise ValueError(f"Unexpected facility: [{self}]")

    @property
    def drive_hours_by_release_district(self) -> dict[UsIxReleaseDistrict, float]:
        """Returns the drive time in hours from each release district's main office
        city to the facility, such as 6.2 from DISTRICT 2 (Lewiston) to ISCI.

        The times come from OpenStreetMap (OSRM) routing run on 2026-10-29, which
        tends to run slower than real driving on rural highways. NICI is measured
        from Cottonwood town, about 3 miles from the facility. ISC has no entry
        because Interstate Compact releases have no Idaho office to measure from.
        """
        if self is UsIxPlacementMatrixFacility.ICIO:
            return {
                UsIxReleaseDistrict.DISTRICT_1: 3.4,
                UsIxReleaseDistrict.DISTRICT_2: 0.9,
                UsIxReleaseDistrict.DISTRICT_3: 5.2,
                UsIxReleaseDistrict.DISTRICT_4: 5.5,
                UsIxReleaseDistrict.DISTRICT_5: 7.6,
                UsIxReleaseDistrict.DISTRICT_6: 9.3,
                UsIxReleaseDistrict.DISTRICT_7: 9.0,
            }
        if self is UsIxPlacementMatrixFacility.ISCC:
            return {
                UsIxReleaseDistrict.DISTRICT_1: 8.5,
                UsIxReleaseDistrict.DISTRICT_2: 6.1,
                UsIxReleaseDistrict.DISTRICT_3: 0.8,
                UsIxReleaseDistrict.DISTRICT_4: 0.4,
                UsIxReleaseDistrict.DISTRICT_5: 2.3,
                UsIxReleaseDistrict.DISTRICT_6: 3.9,
                UsIxReleaseDistrict.DISTRICT_7: 4.6,
            }
        if self is UsIxPlacementMatrixFacility.ISCI:
            return {
                UsIxReleaseDistrict.DISTRICT_1: 8.6,
                UsIxReleaseDistrict.DISTRICT_2: 6.2,
                UsIxReleaseDistrict.DISTRICT_3: 0.8,
                UsIxReleaseDistrict.DISTRICT_4: 0.5,
                UsIxReleaseDistrict.DISTRICT_5: 2.4,
                UsIxReleaseDistrict.DISTRICT_6: 4.0,
                UsIxReleaseDistrict.DISTRICT_7: 4.7,
            }
        if self is UsIxPlacementMatrixFacility.MVTC:
            return {
                UsIxReleaseDistrict.DISTRICT_1: 8.5,
                UsIxReleaseDistrict.DISTRICT_2: 6.1,
                UsIxReleaseDistrict.DISTRICT_3: 0.8,
                UsIxReleaseDistrict.DISTRICT_4: 0.4,
                UsIxReleaseDistrict.DISTRICT_5: 2.3,
                UsIxReleaseDistrict.DISTRICT_6: 3.9,
                UsIxReleaseDistrict.DISTRICT_7: 4.6,
            }
        if self is UsIxPlacementMatrixFacility.NICI:
            return {
                UsIxReleaseDistrict.DISTRICT_1: 3.6,
                UsIxReleaseDistrict.DISTRICT_2: 1.2,
                UsIxReleaseDistrict.DISTRICT_3: 4.3,
                UsIxReleaseDistrict.DISTRICT_4: 4.6,
                UsIxReleaseDistrict.DISTRICT_5: 6.8,
                UsIxReleaseDistrict.DISTRICT_6: 8.4,
                UsIxReleaseDistrict.DISTRICT_7: 9.1,
            }
        raise ValueError(f"Unexpected facility: [{self}]")


# Community Reentry Centers (CRCs) - work release facilities where residents
# can transition back into the community
IX_CRC_FACILITIES = [
    "Nampa Community Reentry Center",
    "East Boise Community Reentry Center",
    "Idaho Falls Community Reentry Center",
    "Twin Falls Community Work Center",
    "Twin Falls Community Reentry Center",
    "Treasure Valley Community Reentry Center",
]

# CRC-like bed facilities - institutional facilities that have CRC-like beds
# for residents who are medically ineligible for CRC work release

# Available to male and female medically ineligible residents (SICI)
IX_CRC_LIKE_BED_FACILITIES_MALE_AND_FEMALE = [
    "SOUTH IDAHO CORRECTIONAL INSTITUTION",
]

# Available only to male medically ineligible residents (ICIO)
IX_CRC_LIKE_BED_FACILITIES_MALE_ONLY = [
    "IDAHO CORRECTIONAL INSTITUTION-OROFINO",
]

# Available only to female medically ineligible residents (PWCC)
IX_CRC_LIKE_BED_FACILITIES_FEMALE_ONLY = [
    "POCATELLO WOMEN'S CORRECTIONAL CENTER",
]

# Combined list of all CRC-like bed facilities
IX_CRC_LIKE_BED_FACILITIES = (
    IX_CRC_LIKE_BED_FACILITIES_MALE_AND_FEMALE
    + IX_CRC_LIKE_BED_FACILITIES_MALE_ONLY
    + IX_CRC_LIKE_BED_FACILITIES_FEMALE_ONLY
)


FACILITY_START_DATE_COLUMN_NAME = "facility_start_date"


def ix_facility_spans_query(facility: UsIxPlacementMatrixFacility) -> str:
    """Returns spans of time someone is housed at a facility."""
    return fix_indent(
        f"""
WITH facility_housing_unit_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_sessions_materialized`
    WHERE state_code = 'US_IX'
        AND facility = '{facility.value}'
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
FROM (
{fix_indent(aggregate_adjacent_spans(table_name="facility_housing_unit_sessions"), indent_level=4)}
)
""",
        indent_level=0,
    )


def us_ix_not_in_facility_criteria_view_builder(
    *,
    criteria_name: str,
    facility_sessions_description: str,
    facility: UsIxPlacementMatrixFacility,
) -> (
    StateSpecificTaskCriteriaBigQueryViewBuilder
    | StateSpecificTaskCriteriaGroupBigQueryViewBuilder
):
    """Returns a criteria builder that is met whenever someone is not in a
    facility. Being in reception at a facility doesn't count as being in the facility.
    """
    reception_housing_unit_prefix = facility.reception_housing_unit_prefix
    if reception_housing_unit_prefix is None:
        return _us_ix_facility_sessions_criteria_view_builder(
            criteria_name=criteria_name,
            description=facility_sessions_description,
            facility=facility,
        )
    return StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        criteria_name=criteria_name,
        logic_type=TaskCriteriaGroupLogicType.OR,
        sub_criteria_list=[
            _us_ix_facility_sessions_criteria_view_builder(
                criteria_name=f"{criteria_name}_FACILITY_SESSIONS",
                description=facility_sessions_description,
                facility=facility,
            ),
            _us_ix_in_reception_unit_criteria_view_builder(
                facility=facility,
                reception_housing_unit_prefix=reception_housing_unit_prefix,
            ),
        ],
    )


def _us_ix_facility_sessions_criteria_view_builder(
    *,
    criteria_name: str,
    description: str,
    facility: UsIxPlacementMatrixFacility,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a criteria builder that is met whenever someone is not housed at a
    facility. Each stay at the facility is a span that does not meet the criteria.
    """

    query_template = fix_indent(
        f"""
WITH facility_spans AS (
{fix_indent(ix_facility_spans_query(facility=facility), indent_level=4)}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(start_date AS {FACILITY_START_DATE_COLUMN_NAME})) AS reason,
    start_date AS {FACILITY_START_DATE_COLUMN_NAME},
FROM facility_spans
""",
        indent_level=0,
    )
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name=FACILITY_START_DATE_COLUMN_NAME,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date the person's current stay at the facility began",
            ),
        ],
    )


def _us_ix_in_reception_unit_criteria_view_builder(
    *,
    facility: UsIxPlacementMatrixFacility,
    reception_housing_unit_prefix: str,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a criteria builder that is met while someone is in a facility's
    Reception and Diagnostic Unit (RDU). Bed moves in the unit are aggregated to one span.
    """
    # TODO(#85650): Source this from hsn_Bed.BedTypeId (4 = "Reception and
    # Diagnostic Unit") once that's in the ingest output, instead of parsing housing
    # unit names.
    query_template = fix_indent(
        f"""
WITH reception_unit_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_sessions_materialized`
    WHERE state_code = 'US_IX'
        AND facility = '{facility.value}'
        AND STARTS_WITH(housing_unit, '{reception_housing_unit_prefix}')
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE AS meets_criteria,
    CAST(NULL AS JSON) AS reason,
FROM (
{fix_indent(aggregate_adjacent_spans("reception_unit_sessions"), indent_level=4)}
)
""",
        indent_level=0,
    )
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=f"US_IX_IN_{facility.name}_RECEPTION_UNIT",
        criteria_spans_query_template=query_template,
        description=(
            f"Spans of time when someone in Idaho is housed in the Reception and "
            f"Diagnostic Unit (RDU) at {facility.value}. Matches housing units starting with "
            f"{reception_housing_unit_prefix}."
        ),
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=False,
        reasons_fields=[],
    )


def us_ix_incarceration_purpose_criteria_view_builder(
    *,
    criteria_name: str,
    description: str,
    purpose: StateSpecializedPurposeForIncarceration,
    purpose_raw_text: str | None,
    stay_label: str,
    stay_start_date_field_name: str,
    stay_end_date_field_name: str,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a criteria builder whose spans cover each continuous stay in
    incarceration periods with the given specialized purpose, removing zero-day spans.
    """
    raw_text_clause = (
        ""
        if purpose_raw_text is None
        else f"AND specialized_purpose_for_incarceration_raw_text = '{purpose_raw_text}'"
    )
    query_template = fix_indent(
        f"""
WITH purpose_periods AS (
    SELECT
        state_code,
        person_id,
        admission_date AS start_date,
        release_date AS end_date,
        specialized_purpose_for_incarceration_raw_text,
    FROM `{{project_id}}.us_ix_normalized_state.state_incarceration_period`
    WHERE specialized_purpose_for_incarceration = '{purpose.value}'
        {raw_text_clause}
        -- Filter out zero-day periods from source data
        AND admission_date IS DISTINCT FROM release_date
),
{create_sub_sessions_with_attributes(table_name="purpose_periods")},
-- Filter out any zero-day spans created by sub_sessions
purpose_spans AS (
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
        end_date,
    FROM sub_sessions_with_attributes
    WHERE start_date IS DISTINCT FROM end_date
),
purpose_stays AS (
{fix_indent(aggregate_adjacent_spans(table_name="purpose_spans"), indent_level=4)}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(
        end_date AS {stay_end_date_field_name},
        start_date AS {stay_start_date_field_name}
    )) AS reason,
    end_date AS {stay_end_date_field_name},
    start_date AS {stay_start_date_field_name},
FROM purpose_stays
""",
        indent_level=0,
    )
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name=stay_end_date_field_name,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description=f"End date of the {stay_label} stay",
            ),
            ReasonsField(
                name=stay_start_date_field_name,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description=f"Start date of the {stay_label} stay",
            ),
        ],
    )


def ix_crc_facilities_in_location_sessions(
    crc_facilities_list: list,
    additional_columns: str = "",
) -> str:
    """
    Returns a SQL query that returns spans of time where someone is in a CRC facility in Idaho.

    Args:
        crc_facilities_list (list): List of CRC facilities to query.
        additional_columns (str): Additional columns to select. Defaults to ''.

    Returns:
        str: SQL query as a string.
    """

    crc_facilities_str = "('" + "', '".join(crc_facilities_list) + "')"

    return f"""SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    {additional_columns if additional_columns else ''}
FROM `{{project_id}}.{{sessions_dataset}}.location_sessions_materialized` 
WHERE state_code = 'US_IX'
    AND facility_name IN {crc_facilities_str}"""
