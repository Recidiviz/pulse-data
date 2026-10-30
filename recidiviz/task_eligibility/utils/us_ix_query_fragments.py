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
"""
Helper SQL queries for Idaho
"""

from enum import Enum
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    nonnull_start_date_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SENTENCE_SESSIONS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_release_district_spans import (
    ADDRESS_RELEASE_DISTRICT_COLUMN_NAME,
    CRC_REQUEST_NOTE_DATE_COLUMN_NAME,
    CRC_REQUEST_RELEASE_DISTRICT_COLUMN_NAME,
    PHYSICAL_ADDRESS_COLUMN_NAME,
    RELEASE_DISTRICT_COLUMN_NAME,
    RELEASE_DISTRICT_SOURCE_COLUMN_NAME,
    SENTENCING_COUNTY_NAME_COLUMN_NAME,
    SENTENCING_COUNTY_RELEASE_DISTRICT_COLUMN_NAME,
    US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME,
    UsIxReleaseDistrict,
    UsIxReleaseDistrictSource,
    release_district_from_crc_request_note_title,
)
from recidiviz.calculator.query.state.views.tasks.assessment_type import AssessmentType
from recidiviz.calculator.query.state.views.tasks.contact_type import ContactType
from recidiviz.calculator.query.state.views.tasks.state_specific_tasks_criteria_builders import (
    non_recurring_contact_compliance_builder,
)
from recidiviz.calculator.query.state.views.tasks.tasks_criteria_utils import (
    get_contact_events_for_type,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    current_violent_statutes_being_served,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.observations.span_observation_big_query_view_builder import fix_indent
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)

# Days before a supervision session start within which an assessment still counts
# toward that session. Shared by each initial-assessment trigger and its annual
# reassessment criteria so both recognize the same pre-session assessments.
US_IX_LSIR_ASSESSMENT_LOOKBACK_DAYS = 90
US_IX_STABLE_ASSESSMENT_LOOKBACK_DAYS = 60

# Idaho treats an LSI-R done for a pre-sentence investigation (PSI) as valid for a
# year, so a PSI LSI-R counts toward a session that starts up to this many days later.
US_IX_PSI_LSIR_LOOKBACK_DAYS = 365

# LSI-Rs are sometimes entered a few days after the PSI report is marked complete.
_US_IX_PSI_COMPLETED_DATE_GRACE_DAYS = 7

# LSI-R assessments completed as part of a PSI, one row per person and assessment date.
US_IX_PSI_LSIR_ASSESSMENTS_QUERY = f"""
    SELECT DISTINCT
        lsir.state_code,
        lsir.person_id,
        lsir.assessment_date,
    FROM `{{project_id}}.sessions.assessment_score_sessions_materialized` lsir
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        ON peid.person_id = lsir.person_id
        AND peid.state_code = 'US_IX'
        AND peid.id_type = 'US_IX_DOC'
    INNER JOIN `{{project_id}}.us_ix_raw_data_up_to_date_views.com_PSIReport_latest` psi
        ON psi.OffenderId = peid.external_id
        AND lsir.assessment_date BETWEEN DATE(psi.OrderDate)
            AND DATE_ADD(DATE(psi.CompletedDate), INTERVAL {_US_IX_PSI_COMPLETED_DATE_GRACE_DAYS} DAY)
    WHERE lsir.state_code = 'US_IX'
        AND lsir.assessment_type = 'LSIR'
        AND lsir.assessment_class = 'RISK'
"""


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

DETAINER_TYPE_LST_CRC = ["59"]
HOLD_TYPE_LST_CRC = ["84", "112", "85", "61", "87", "86", "62", "88", "67", "83", "113"]
# detainer types related to felonies and immigration (https://docs.google.com/spreadsheets/d/1sV6BLWQQiQ4mrtzWJiooaHzO_sgk-bNkCMdwolQ8aVk/edit?usp=sharing)
DETAINER_TYPE_LST_CLD = [
    "2",
    "59",
    "60",
    "76",
    "78",
    "84",
    "85",
    "87",
    "98",
    "99",
    "101",
    "102",
    "108",
    "112",
    "115",
    "117",
    "121",
]

IX_STATE_CODE_WHERE_CLAUSE = "WHERE state_code = 'US_IX'"
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


def detainer_span(types_to_include_lst: List[str]) -> str:
    """
    Retrieves detainer spans information based on the specified detainer type IDs.

    Args:
        types_to_include_lst (List[str]): A list of detainer type IDs to include in the query.

    Returns:
        str: SQL query string for retrieving detainer spans with the specified detainer types.

    Example:
        types_to_include = ['73', '23']
        query = detainer_span(types_to_include)
    """
    reformatted_types_to_include_lst = "('" + "', '".join(types_to_include_lst) + "')"
    return f"""WITH
    detainer_cte AS (
        SELECT *
        FROM `{{project_id}}.{{analyst_dataset}}.us_ix_detainer_spans_materialized`
        WHERE DetainerTypeId IN {reformatted_types_to_include_lst}),
          {create_sub_sessions_with_attributes('detainer_cte')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE as meets_criteria,
    TO_JSON(STRUCT(start_date AS latest_detainer_start_date,
        DetainerTypeDesc AS latest_detainer_type,
        DetainerStatusDesc AS latest_detainer_status)) AS reason,
    start_date AS latest_detainer_start_date,
    DetainerTypeDesc AS latest_detainer_type,
    DetainerStatusDesc AS latest_detainer_status,
FROM
    sub_sessions_with_attributes
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY start_date DESC)=1
    """


# This pattern should match text enclosed within angle brackets < >,
#   curly braces { }, or the pattern &nbsp
ESCAPE_CURLY_AND_ANGLE_BRACKETS_REGEX = "[<{{]([^>}}]*)[>}}]|&nbsp"


def ix_offender_alerts_case_notes(
    date_part: str = "MONTH",
    date_interval: str = "6",
    where_clause: str = "",
    indent_level: int = 8,
) -> str:
    """
    Returns a SQL query that returns case notes for offender alerts in Idaho.
    Args:
        date_part (str): Date part to use for the date interval. Defaults to 'MONTH'.
        date_interval (str): Date interval to use for the date part. Defaults to '6'.
        where_clause (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 2.
    Returns:
        str: SQL query as a string.
    """
    query = f"""
        SELECT
            OffenderId AS external_id,
            "Alerts" AS criteria,
            AlertDesc AS note_title,
            REGEXP_REPLACE(Notes, r'{ESCAPE_CURLY_AND_ANGLE_BRACKETS_REGEX}', '') AS note_body,
            start_date AS event_date,
        FROM (
            SELECT  
                *,
                CAST(LEFT(StartDate, 10) AS DATE) AS start_date,
                {nonnull_end_date_clause('CAST(LEFT(EndDate, 10) AS DATE)')} AS end_date,
            FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Offender_Alert_latest` oa
            INNER JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Alert_latest` ia
                USING (AlertId)
            {where_clause})
        WHERE end_date > DATE_SUB(CURRENT_DATE, INTERVAL {date_interval} {date_part})"""

    return fix_indent(query, indent_level=indent_level)


INSTITUTIONAL_BEHAVIOR_NOTES_STR = "Institutional Behavior Notes (in the past 6 months)"
I9_NOTES_STR = "I-9 Documents Notes"
WORK_HISTORY_STR = "Work History (in the past 5 years)"
MEDICAL_CLEARANCE_STR = "Medical Clearance (in the past 6 months)"
RELEASE_INFORMATION_STR = "Release District Information"
# TODO(#30190) - add new contact mode types
RELEASE_INFORMATION_CONTACT_MODES = """(
                    "CRC Request - D1 Release",
                    "CRC Request - D2 Release",
                    "CRC Request - D3 Release",
                    "CRC Request - D4 Release",
                    "CRC Request - D5 Release",
                    "CRC Request - D6 Release",
                    "CRC Request - D7 Release",
                    "CRC Request - ISC Release")"""
CRC_INFORMATION_STR = "Additional CRC Information (in the past 6 months)"
CRC_INFORMATION_CONTACT_MODES = """('CRC Termer Approved',
                    'CRC Termer Denied', 
                    'CRC DIV Approved', 
                    'CRC DIV Denied')"""
I9_NOTE_TX_REGEX = "|".join(
    ["I9", "I-9", "I- 9", "I - 9", "I -9", "I - 9", "I- 9", "I -9"]
)
MEDICAL_CLEARANCE_TX_REGEX = "|".join(["MEDICALLY CLEAR", "MEDICAL CLEAR"])


def ix_general_case_notes(
    criteria_str: str,
    where_clause_addition: str = "",
    in_the_past_x_months: Optional[int] = None,
    indent_level: int = 8,
    *,
    include_note_type: bool = False,
) -> str:
    """
    Returns a SQL query that returns case notes for a specified contact mode in Idaho.

    Args:
        criteria_str (str): Criteria to use for the criteria column.
        in_the_past_x_months (int): Number of months to use for the date interval.
            Defaults to 6.
        where_clause_addition (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 2.
        include_note_type (bool): Whether to add a note_type column holding the
            note's Atlas note type (e.g. Facility Notes, Supervision Notes). Off by
            default, so callers that union this with other case note queries keep
            the same columns.
    Returns:
        str: SQL query as a string.
    """
    note_type_column = (
        "\n                note_type.NoteTypeDesc AS note_type,"
        if include_note_type
        else ""
    )
    group_by_columns = "1,2,3,4,5,6" if include_note_type else "1,2,3,4,5"
    date_filter_clause = ""
    if in_the_past_x_months is not None:
        date_filter_clause = f"""
            WHERE DATE_DIFF(CURRENT_DATE('US/Pacific'), event_date, MONTH)
            - IF(EXTRACT(DAY FROM event_date) > EXTRACT(DAY FROM CURRENT_DATE('US/Pacific')), 1, 0)
            <= {in_the_past_x_months}"""
    query = f"""
        SELECT *
        FROM (
            SELECT
                info.OffenderId AS external_id,
                '{criteria_str}' AS criteria,
                note_type_cm.ContactModeDesc AS note_title,
                REGEXP_REPLACE(note.Details, r'{ESCAPE_CURLY_AND_ANGLE_BRACKETS_REGEX}', '') AS note_body,
                SAFE_CAST(LEFT(info.NoteDate, 10) AS DATE) AS event_date,{note_type_column}
            FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNote_latest` note
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNoteInfo_latest` info
                USING (OffenderNoteInfoId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ref_NoteType_latest` note_type
                USING (NoteTypeId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_OffenderNoteInfo_ContactMode_latest` info_cm
                USING (OffenderNoteInfoId)
            LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_ContactMode_latest` note_type_cm
                USING (ContactModeId)
            WHERE OffenderNoteStatusId = '2' -- Posted
                {where_clause_addition}
            GROUP BY {group_by_columns}
        )
        {date_filter_clause}"""

    return fix_indent(query, indent_level=indent_level)


NOTE_TITLE_REGEX = "r'^{{note_title:(.*?)}}'"
NOTE_BODY_REGEX = " r'{{note:((?s:.*))}}'"


def ix_fuzzy_matched_case_notes(where_clause: str = "", indent_level: int = 8) -> str:
    """
    Returns a SQL query that returns fuzzy matched case notes filtered by type in Idaho

    Args:
        where_clause (str): Where clause to use in the query. Defaults to ''.
        indent_level (int): Indent level for the query. Defaults to 8.
    """
    query = f"""
        SELECT *
        FROM (
            SELECT 
                person_external_id AS external_id,
                'New criminal activity check' AS criteria,
                COALESCE(REGEXP_EXTRACT(Details, {{note_title_regex}}),
                         '') AS note_title,
                COALESCE(REGEXP_EXTRACT(REGEXP_REPLACE(Details, '<[^>]*>', ''), {{note_body_regex}}), 
                         REGEXP_REPLACE(Details, '<[^>]*>', '')) AS note_body,
                NoteDate AS event_date,
            FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities`
            {where_clause}
        )
        WHERE (DATE_DIFF(CURRENT_DATE('US/Pacific'), event_date, MONTH)
                    - IF(EXTRACT(DAY FROM event_date) > EXTRACT(DAY FROM CURRENT_DATE('US/Pacific')),
                        1, 0)) <= 3"""

    return fix_indent(query, indent_level=indent_level)


def escape_absconsion_or_eluding_police_case_notes(
    criteria_column_str: str = "Escape, Absconsion or Eluding Police history (in the past 10 years)",
    indent_level: int = 8,
) -> str:
    """
    Returns a SQL query that returns case notes for escape, absconsion or eluding police in Idaho.

    Args:
        criteria_column_str (str): Criteria to use for the criteria column. Defaults to
            'Escape, Absconsion or Eluding Police history (in the past 10 years').
        indent_level (int): Indent level for the query. Defaults to 8.

    Returns:
        str: SQL query as a string.
    """

    query = f"""
    SELECT
        pei.external_id,
        '{criteria_column_str}' AS criteria,
        description AS note_title,
        statute AS note_body,
        {extract_object_from_json(object_column = 'most_recent_statute_date', 
                                object_type='DATE')} AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix_dataset}}.no_escape_offense_within_10_years_materialized` ne,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses)) AS statute,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses_descriptions)) AS description
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON ne.person_id = pei.person_id
         AND ne.state_code = pei.state_code
         AND pei.id_type = 'US_IX_DOC'
    WHERE CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
        AND meets_criteria IS FALSE
    GROUP BY 1,2,3,4,5

    UNION ALL 

    SELECT 
        pei.external_id,
        '{criteria_column_str}' AS criteria,
        '' AS note_title,
        'ABSCONSION' AS note_body,
        {extract_object_from_json(object_column = 'most_recent_absconded_date', 
                                object_type='DATE')} AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_dataset}}.no_absconsion_within_10_years_materialized` na
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON na.person_id = pei.person_id
         AND na.state_code = pei.state_code
         AND pei.id_type = 'US_IX_DOC'
    WHERE pei.state_code = 'US_IX'
        AND CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
            AND meets_criteria IS FALSE

    UNION ALL

    SELECT
        pei.external_id,
        '{criteria_column_str}' AS criteria,
        description AS note_title,
        statute AS note_body,
        {extract_object_from_json(object_column = 'most_recent_statute_date', 
                                object_type='DATE')} AS event_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_ix_dataset}}.no_eluding_police_offense_within_10_years_materialized` ne,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses)) AS statute,
    UNNEST(JSON_VALUE_ARRAY(reason.ineligible_offenses_descriptions)) AS description
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON ne.person_id = pei.person_id
            AND ne.state_code = pei.state_code
            AND pei.id_type = 'US_IX_DOC'
    WHERE CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_clause('end_date')}
        AND meets_criteria IS FALSE
    GROUP BY 1,2,3,4,5"""

    return fix_indent(query, indent_level=indent_level)


def detainer_case_notes(
    criteria_column: str = "Detainers", indent_level: int = 8
) -> str:
    """
    Returns a SQL query that returns case notes for detainers in Idaho.
    Args:

        criteria_column (str): Criteria to use for the criteria column. Defaults to 'Detainers'.
        indent_level (int): Indent level for the query. Defaults to 8.
    """

    query = f"""
SELECT
    pei.external_id,
    '{criteria_column}' AS criteria,
    CONCAT(DetainerTypeDesc, ' - ', DetainerStatusDesc) AS note_title,
    Comments AS note_body,
    start_date AS event_date,
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_detainer_spans_materialized` det
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON det.person_id = pei.person_id
        AND det.state_code = pei.state_code
        AND pei.id_type = 'US_IX_DOC'"""

    return fix_indent(query, indent_level=indent_level)


def lsir_spans() -> str:
    """
    Returns a SQL query that returns LSIR spans in Idaho.

    Note: Confirmed on 12/8/25 that Idaho uses gender (not sex) to classify individual LSIR scores.
    """

    return f"""LSIR_level_gender AS(
  /* This CTE creates a view of LSIR-score level by gender according to updated
  Idaho Supervision Categories (07/21/2020) */
  SELECT
      score.person_id,
      score.state_code,
      score.assessment_date AS score_start_date,
      {nonnull_end_date_clause('score.score_end_date_exclusive')} AS score_end_date,
      ses.start_date AS supervision_start_date,
      ses.end_date_exclusive AS supervision_end_date,
      CASE
          WHEN ((gender != "MALE" OR gender IS NULL) AND assessment_score <=22) THEN "LOW"
          WHEN ((gender != "MALE" OR gender IS NULL)
                                AND (assessment_score BETWEEN 23 AND 30)) THEN "MODERATE"
          WHEN (gender = "MALE" AND assessment_score <=20) THEN "LOW"
          WHEN (gender = "MALE" AND (assessment_score BETWEEN 21 AND 28)) THEN "MODERATE"
          ELSE "HIGH"
          END AS lsir_level
  FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`ses
    ON score.state_code = ses.state_code
    AND score.person_id = ses.person_id
    --only consider scores relevant in the supervision session during which they occur 
    AND ses.start_date < {nonnull_end_date_clause('score.score_end_date_exclusive')}
    AND score.assessment_date < {nonnull_end_date_clause('ses.end_date_exclusive')}
     LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` info
      ON score.state_code = info.state_code
      AND score.person_id = info.person_id
  WHERE score.state_code = 'US_IX' 
      AND assessment_type = 'LSIR'
),
grp_starts as (
   /* This CTE identifies when the LSI-R level changes within a supervision session. 
   grp_start is 1 at the beginning of each new level span and 0 for consecutive periods with the same LSI-R score.*/
  SELECT 
      state_code,
      person_id,
      lsir_level, 
      supervision_start_date,
      supervision_end_date,
      score_start_date,
      score_end_date,
      IF(lsir_level = LAG(lsir_level) OVER supervision_window, 0, 1) AS grp_start
  FROM LSIR_level_gender
  WINDOW supervision_window AS (
    PARTITION BY person_id, supervision_start_date ORDER BY score_start_date
  )
),
grps as (
    /* This CTE sums grp_starts to group together adjacent periods where the LSI-R score is the same */
    SELECT
        state_code,
        person_id,
        lsir_level, 
        supervision_start_date,
        supervision_end_date,
        score_start_date,
        score_end_date,
        SUM(grp_start) OVER(ORDER BY person_id, supervision_start_date, score_start_date) AS grp
    FROM grp_starts
),
lsir_spans AS (
    /* This CTE actually combines adjacent periods where the LSI-R score is the same by 
    choosing min start and max end date.*/
    SELECT 
        state_code, 
        person_id,
        lsir_level,
        MIN(score_start_date) AS score_span_start_date,
        --end span date on supervision session end date, or score end date, whichever comes first
        MAX(LEAST(score_end_date,{nonnull_end_date_clause('supervision_end_date')})) AS score_span_end_date
    FROM grps
    GROUP BY grp, state_code, person_id, lsir_level)"""


def us_ix_dors_query(*, dor_classes: list[str]) -> str:
    """Returns a SQL query for the US_IX DORs of the given classes, one row per DOR, read
    from normalized state. Dismissed and not-guilty DORs are left out; pending DORs and
    informal resolutions are kept. Reports with no DA case are also left out, since
    nothing records whether they were decided. The offense type name and the description
    come from incident_metadata, which keeps IDOC's original casing for display.

    For example, a resident with a pending class B DOR and a dismissed class A DOR gets
    one row (some columns abridged):

        person_id,dor_class,offense_type_name,dor_description,incident_date,hearing_date,is_pending
        123,B,Disobedience to Orders 2,Resident refused to return to the assigned cell.,2026-08-01,,true
    """
    return f"""
    SELECT
        incident.state_code,
        incident.person_id,
        incident.incarceration_incident_id,
        -- Report-backed DORs use the report id as their external_id.
        incident.external_id AS incident_external_id,
        NULLIF(JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.DACaseId'), '') AS da_case_id,
        incident.incident_severity_raw_text AS dor_class,
        JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.OffenseTypeName') AS offense_type_name,
        JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.IncidentDetails') AS dor_description,
        incident.incident_date,
        CAST(
            LEFT(NULLIF(JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.HearingDate'), ''), 10)
            AS DATE
        ) AS hearing_date,
        incident.facility,
        incident.incarceration_incident_id IN (
            SELECT incarceration_incident_id
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
            WHERE state_code = 'US_IX'
                AND outcome_type = 'PENDING'
        ) AS is_pending,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` incident
    WHERE incident.state_code = 'US_IX'
        AND incident.incident_severity_raw_text
            IN ({list_to_query_string(dor_classes, quoted=True, single_quote=True)})
        AND NULLIF(JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.DACaseId'), '') IS NOT NULL
        AND incident.incarceration_incident_id NOT IN (
            SELECT incarceration_incident_id
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
            WHERE state_code = 'US_IX'
                AND outcome_type IN ('DISMISSED', 'NOT_GUILTY')
        )"""


def dor_case_notes_query(*, in_the_past_x_months: int, indent_level: int) -> str:
    """Returns a SQL query for the class A, B and C DORs from the past
    in_the_past_x_months months, as opportunity case notes keyed on the resident's
    current US_IX_DOC display id."""
    query = f"""
    SELECT
        pei.external_id,
        'Disciplinary Offense Reports (in the past {in_the_past_x_months} months)' AS criteria,
        CONCAT('Class ', dor.dor_class, ': ', dor.offense_type_name) AS note_title,
        dor.dor_description AS note_body,
        dor.incident_date AS event_date,
    FROM (
{fix_indent(us_ix_dors_query(dor_classes=["A", "B", "C"]), indent_level=8)}
    ) dor
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON pei.person_id = dor.person_id
        AND pei.id_type = 'US_IX_DOC'
        AND pei.is_current_display_id_for_type
    WHERE dor.incident_date > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {in_the_past_x_months} MONTH)"""
    return fix_indent(query, indent_level=indent_level)


def program_enrollment_query(indent_level: int = 8) -> str:
    """
    Returns a SQL query that selects information about program enrollments. Some facts:
    - The query includes the offender ID, criteria, note title, note body, and event date.
    - Only records with an enrollment status of 'Complete', 'Enrolled', or 'Waitlisted'
    are included.
    - Only programs that were started at current incarceration periods are included
    """
    query = """
    SELECT 
        ce.OffenderId AS external_id,
        "Program enrollment" AS criteria,
        CourseNameName AS note_title,
        CONCAT(ces.OfdEnrollmentStatusDesc, ' - ', CourseSectionNameName) AS note_body,
        event_date
    FROM (
        SELECT 
            *,
            COALESCE( CAST(LEFT(ce.EndDate, 10) AS DATE),
                CAST(LEFT(ce.StartDate, 10) AS DATE),
                CAST(LEFT(ce.PendingEnrollmentDate, 10) AS DATE)) AS event_date
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_OfdCourseEnrollment_latest` ce  
    ) ce
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_OfdEnrollmentStatus_latest` ces
    ON 
        ce.OfdEnrollmentStatusId = ces.OfdEnrollmentStatusId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_Course_latest` co
    ON 
        ce.CourseId = co.CourseId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseName_latest` con
    ON 
        co.CourseNameId = con.CourseNameId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseSection_latest` cs
    ON 
        cs.CourseSectionId = ce.CourseSectionId
    LEFT JOIN 
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.crs_CourseSectionName_latest` csn
    ON 
        cs.CourseSectionNameId = csn.CourseSectionNameId
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON ce.OffenderId = pei.external_id
         AND pei.state_code = 'US_IX'
         AND pei.id_type = 'US_IX_DOC'
    -- Only inlcude enrollments that started during the current incarceration period
    INNER JOIN (
        SELECT person_id, start_date
        FROM `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` 
        WHERE state_code = 'US_IX'
            AND compartment_level_0 = 'INCARCERATION'
            AND CURRENT_DATE('US/Pacific') < IFNULL(end_date, '9999-12-31')
    ) ss
    ON pei.person_id = ss.person_id
        AND ss.start_date < event_date
    WHERE 
        ce.OfdEnrollmentStatusId IN ('1', '2', '3')
    """

    return fix_indent(query, indent_level=indent_level)


def victim_alert_notes(indent_level: int = 8) -> str:
    """
    Returns a SQL query that selects information about victim alerts.
    Args:
        indent_level (int): Indent level for the query. Defaults to 8.
    """

    query = """
    SELECT
        OffenderId AS external_id,
        'Victim Alerts' AS criteria,
        'Attention' AS note_title,
        Notes AS note_body,
        SAFE_CAST(LEFT(StartDate, 10) AS DATE) AS event_date
    FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_Offender_Alert_latest`
    -- Victim alerts
    WHERE AlertId = '133'"""

    return fix_indent(query, indent_level=indent_level)


def supervision_level_criteria_query(
    excluded_levels: List[str],
) -> str:
    return f"""
#TODO(#22511) refactor to build off of a general criteria view builder
WITH so_spans AS (
SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
    WHERE compartment_level_1 = 'SUPERVISION'
    AND correctional_level_raw_text IN {tuple(excluded_levels)}
    AND state_code = 'US_IX'
)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
    TO_JSON(STRUCT(TRUE AS supervision_level_is_so)) AS reason,
    TRUE AS supervision_level_is_so,
    FROM ({aggregate_adjacent_spans(table_name='so_spans')})
"""


def eprd_cte() -> str:
    """
    Defines a CTE that calculates the Earliest Possible Release Date (EPRD) for individuals in Idaho.

    The EPRD is calculated as specified below:
        EPRD = TPD, if the TPD exists
        EPRD = FTRD, if both TPD and PHD do not exist
        EPRD = PED, if TPD does not exist, the PHD is prior to the PED, and the PED after today.
        EPRD = PHD, if TPD does not exist and PHD is after PED.
    """

    return f"""ped_tpd_ftrd_spans AS (
SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date_exclusive AS end_date,
    span.group_parole_eligibility_date AS parole_eligibility_date,
    span.group_projected_parole_release_date AS tentative_parole_date,
    span.group_projected_full_term_release_date_max AS full_term_release_date
FROM `{{project_id}}.{{sentence_sessions_v2_dataset}}.person_projected_date_sessions_materialized` span
WHERE span.state_code = 'US_IX'
),
ped_tpd_phd_ftrd_spans AS (
SELECT
    state_code,
    person_id,
    start_date,
    -- Only retain PEDs in the future
    LEAST({nonnull_end_date_clause('end_date')}, parole_eligibility_date) AS end_date,
    parole_eligibility_date,
    NULL AS tentative_parole_date,
    NULL AS parole_hearing_date,
    NULL AS full_term_release_date
FROM ped_tpd_ftrd_spans
WHERE parole_eligibility_date IS NOT NULL
  -- Avoid zero-day spans when parole_eligibility_date <= start_date
  AND parole_eligibility_date > start_date

UNION ALL

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    NULL AS parole_eligibility_date,
    tentative_parole_date,
    NULL AS parole_hearing_date,
    full_term_release_date
FROM ped_tpd_ftrd_spans ped

UNION ALL

SELECT 
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    NULL AS parole_eligibility_date,
    NULL AS tentative_parole_date,
    pds.initial_parole_hearing_date AS parole_hearing_date,
    NULL AS full_term_release_date
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds

UNION ALL

SELECT 
    pds.state_code,
    pds.person_id,
    start_date,
    end_date_exclusive AS end_date,
    NULL AS parole_eligibility_date,
    NULL AS tentative_parole_date,
    pds.next_parole_hearing_date AS parole_hearing_date,
    NULL AS full_term_release_date
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds
),
{create_sub_sessions_with_attributes(
table_name="ped_tpd_phd_ftrd_spans"
)},
grouped_sub_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    MIN(parole_eligibility_date) AS parole_eligibility_date, 
    MIN(tentative_parole_date) AS tentative_parole_date, 
    --take the MIN of all future parole hearing dates, or if none exist, the MAX of all past parole hearing dates
    --this only works based on the subsessionizing done in us_ix_parole_dates_spans_preprocessing_materialized
    COALESCE(
        MIN(IF(parole_hearing_date > start_date, parole_hearing_date, NULL)),
        MAX(parole_hearing_date)
    ) AS parole_hearing_date,
    MIN(full_term_release_date) AS full_term_release_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
),
eprd_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    parole_eligibility_date,
    tentative_parole_date,
    parole_hearing_date,
    full_term_release_date,
    CASE
        WHEN tentative_parole_date IS NOT NULL THEN tentative_parole_date
        WHEN parole_hearing_date IS NULL THEN full_term_release_date
        WHEN parole_eligibility_date > parole_hearing_date THEN parole_eligibility_date
        WHEN (parole_hearing_date >= parole_eligibility_date OR parole_eligibility_date IS NULL) THEN parole_hearing_date
        ELSE NULL
    END AS earliest_possible_release_date
FROM grouped_sub_sessions
)"""


def eprd_within_x_months_criteria_view_builder(
    *,
    criteria_name: str,
    description: str,
    num_months: int,
    meets_criteria_bool: bool = True,
    meets_criteria_default: bool = False,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a US_IX criteria view builder for whether someone is (or, when
    meets_criteria_bool is False, is not) within `num_months` of their upcoming
    Earliest Possible Release Date (EPRD) and has not yet passed that date.

    Args:
        criteria_name (str): Name of the criteria.
        description (str): Description of the criteria.
        num_months (int): Number of months to use for the leading window time.
        meets_criteria_bool (bool): Whether spans meet or do not meet the criteria.
            Defaults to True.
        meets_criteria_default (bool): meets_criteria value to use for periods the query
            produces no span for (e.g. when there is no upcoming EPRD). Defaults to
            False.
    """
    meets_criteria_condition = "" if meets_criteria_bool else "NOT"
    query_template = fix_indent(
        f"""
    WITH {eprd_cte()},
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            --crop spans so that EPRDs are always upcoming
            LEAST({nonnull_end_date_clause('end_date')}, earliest_possible_release_date) AS end_datetime,
            earliest_possible_release_date AS critical_date,
            parole_eligibility_date,
            tentative_parole_date,
            parole_hearing_date,
            full_term_release_date,
            earliest_possible_release_date
        FROM eprd_sessions
        --only include spans where EPRD is upcoming
        WHERE earliest_possible_release_date > start_date
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time=num_months,
                                        attributes=["parole_eligibility_date",
                                                    "tentative_parole_date",
                                                    "parole_hearing_date",
                                                    "full_term_release_date",
                                                    "earliest_possible_release_date"],
                                        date_part="MONTH")}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        {meets_criteria_condition} critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(parole_eligibility_date,
                    tentative_parole_date,
                    parole_hearing_date,
                    full_term_release_date,
                    earliest_possible_release_date)) AS reason,
        parole_eligibility_date,
        tentative_parole_date,
        parole_hearing_date,
        full_term_release_date,
        earliest_possible_release_date
    FROM critical_date_has_passed_spans
    WHERE start_date IS DISTINCT FROM end_date""",
        indent_level=8,
    )
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sentence_sessions_v2_dataset=SENTENCE_SESSIONS_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        state_code=StateCode.US_IX,
        meets_criteria_default=meets_criteria_default,
        reasons_fields=[
            ReasonsField(
                name="parole_eligibility_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Parole Eligibility Date (PED): The date on which the person becomes eligible for parole.",
            ),
            ReasonsField(
                name="tentative_parole_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Tentative Parole Date (TPD): The date on which the person is tentatively scheduled for parole.",
            ),
            ReasonsField(
                name="parole_hearing_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Parole Hearing Date (PHD): The date on which the person is scheduled for a parole hearing.",
            ),
            ReasonsField(
                name="full_term_release_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Full Term Release Date (FTRD): The maximum full term release date.",
            ),
            ReasonsField(
                name="earliest_possible_release_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Earliest Possible Release Date (EPRD): The earliest possible date on which the person can be released from incarceration.",
            ),
        ],
    )


def us_ix_active_supervision_population_view_builder(
    population_name: str,
    description: str,
    case_types: List[str],
    supervision_levels: List[str],
    additional_where_clause: Optional[str] = None,
) -> StateSpecificTaskCandidatePopulationBigQueryViewBuilder:
    """
    Creates a VIEW_BUILDER for Idaho active supervision population with specified filters.

    This includes people on PROBATION, PAROLE, DUAL, or COMMUNITY_CONFINEMENT
    with specified case types and supervision levels.

    Args:
        population_name: The name of the population (e.g., 'US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS')
        description: The description/docstring for the population
        case_types: List of case types to include (e.g., ['GENERAL', 'SEX_OFFENSE'])
        supervision_levels: List of supervision levels to include (e.g., ['MINIMUM', 'MEDIUM', 'HIGH'])
        additional_where_clause: Optional additional WHERE clause to apply to compartment_sub_sessions.
            Should start with 'AND'. Example: "AND css.sex = 'MALE'"
    """
    case_types_str = "('" + "', '".join(case_types) + "')"
    supervision_levels_str = "('" + "', '".join(supervision_levels) + "')"

    # Build additional where clause if specified
    where_clause_addition = additional_where_clause or ""

    query_template = f"""WITH active_supervision_population AS (
    -- Active supervision population on PROBATION, PAROLE, or DUAL supervision
    SELECT
        css.state_code,
        css.person_id,
        css.start_date,
        css.end_date_exclusive AS end_date,
        css.compartment_level_1,
        css.compartment_level_2,
        SAFE_CAST(NULL AS STRING) AS case_type,
        SAFE_CAST(NULL AS STRING) AS supervision_level
    FROM
        `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
    WHERE css.compartment_level_1 IN ('SUPERVISION')
        AND css.metric_source != "INFERRED"
        AND css.compartment_level_2 IN ('PROBATION', 'PAROLE', 'DUAL', 'COMMUNITY_CONFINEMENT')
        AND css.correctional_level NOT IN ('IN_CUSTODY','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN')
        AND css.start_date >= '1900-01-01'
        {where_clause_addition}
),

supervision_case_and_level AS (
    -- Filter by case types and supervision levels
    SELECT
        ctsl.state_code,
        ctsl.person_id,
        ctsl.start_date,
        ctsl.end_date,
        SAFE_CAST(NULL AS STRING) AS compartment_level_1,
        SAFE_CAST(NULL AS STRING) AS compartment_level_2,
        ctsl.case_type,
        ctsl.supervision_level,
    FROM `{{project_id}}.tasks_views.us_ix_case_type_supervision_level_spans_materialized` ctsl
        WHERE ctsl.case_type IN {case_types_str}
            AND ctsl.supervision_level IN {supervision_levels_str}
),

combine AS (
    SELECT *
    FROM active_supervision_population

    UNION ALL

    SELECT *
    FROM supervision_case_and_level
),

{create_sub_sessions_with_attributes(table_name="combine")}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
-- We only want spans where both criteria are met
HAVING COUNT(*) >= 2
"""

    return StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        population_name=population_name,
        population_spans_query_template=query_template,
        description=description,
    )


def us_ix_supervision_start_spans_query() -> str:
    """Returns a SQL query that selects supervision start spans in Idaho.

    Each row represents a supervision period with the earliest
    compartment_level_1_super_session start date and the
    supervision_super_session end date. Used as the base contact-period
    spans query for initial assessment triggers built with
    non_recurring_contact_compliance_builder.
    """
    return f"""
    SELECT
        css.state_code,
        css.person_id,
        MIN(css.start_date) AS start_date,
        sss.end_date_exclusive AS end_date,
    FROM `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` css
    LEFT JOIN `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
        ON css.state_code = sss.state_code
            AND css.person_id = sss.person_id
            AND css.start_date BETWEEN sss.start_date AND {nonnull_end_date_clause('sss.end_date')}
    WHERE css.state_code = 'US_IX'
        AND css.start_reason IN ('COURT_SENTENCE', 'RELEASE_FROM_INCARCERATION')
        AND css.compartment_level_1 = 'SUPERVISION'
    GROUP BY css.state_code, css.person_id, sss.end_date_exclusive
"""


def us_ix_annual_assessment_criteria_view_builder(
    criteria_name: str,
    description: str,
    assessment_type: AssessmentType,
    assessment_class: str,
    assessment_lookback_days: int,
    include_psi_lsir_assessments: bool,
    include_supervision_contact_reasons: Optional[List[str]] = None,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Creates a VIEW_BUILDER for annual assessment criteria in Idaho.

    This criteria shows spans of time for which supervision clients need an assessment
    reassessment. This should happen every 365 days after the last assessment, and is
    triggered 30 days before that date.

    Args:
        criteria_name: The name of the criteria (e.g., 'US_IX_IS_MISSING_ANNUAL_LSIR_ASSESSMENT')
        description: The description/docstring for the criteria
        assessment_type: The compliance AssessmentType this criterion is tied to (e.g.,
            AssessmentType.LSIR, AssessmentType.STABLE). Its value doubles as the
            `state_assessment.assessment_type` value to filter on.
        assessment_class: The assessment class to filter (e.g., 'RISK', 'SEX_OFFENSE')
        assessment_lookback_days: Days before a session start within which an assessment
            still anchors that session's reassessment clock. Mirrors the initial
            trigger's `contact_date_lookback_days`.
        include_psi_lsir_assessments: Whether LSI-Rs done for a pre-sentence
            investigation anchor the reassessment clock of a session that starts up to
            US_IX_PSI_LSIR_LOOKBACK_DAYS later. Only valid for LSI-R assessments.
        include_supervision_contact_reasons: Optional list of supervision contact reasons to also
            consider as valid assessments (e.g., ['LSI_R_REVIEW', 'INITIAL,LSI_R_REVIEW']).
            When provided, supervision contacts with these contact_reasons and status='COMPLETED'
            will be included via UNION ALL.
    """
    if include_psi_lsir_assessments and assessment_type is not AssessmentType.LSIR:
        raise ValueError(
            f"include_psi_lsir_assessments is only valid for LSIR assessments, found "
            f"assessment_type [{assessment_type.value}] for criteria [{criteria_name}]"
        )
    # US_IX's assessment ingest values match their AssessmentType codes.
    state_assessment_type = assessment_type.value

    # Build optional supervision contact query
    supervision_contact_union = ""
    if include_supervision_contact_reasons:
        contact_reasons_str = list_to_query_string(
            include_supervision_contact_reasons, quoted=True, single_quote=True
        )
        supervision_contact_union = f"""
    UNION ALL

    -- Include supervision contacts with the specified contact_reasons as valid assessments
    SELECT
        state_code,
        person_id,
        contact_date AS assessment_date,
        {assessment_lookback_days} AS lookback_days,
    FROM `{{project_id}}.normalized_state.state_supervision_contact`
    WHERE state_code = 'US_IX'
        AND contact_reason_raw_text IN ({contact_reasons_str})
        AND status = 'COMPLETED'"""

    psi_lsir_union = ""
    if include_psi_lsir_assessments:
        psi_lsir_union = f"""
    UNION ALL

    SELECT
        state_code,
        person_id,
        assessment_date,
        {US_IX_PSI_LSIR_LOOKBACK_DAYS} AS lookback_days,
    FROM ({US_IX_PSI_LSIR_ASSESSMENTS_QUERY})"""

    query_template = f"""
WITH supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        -- Previous session's end, so the lookback below can't reach into a prior
        -- session and attach one assessment to two sessions.
        LAG(end_date_exclusive) OVER (
            PARTITION BY state_code, person_id ORDER BY start_date
        ) AS prev_session_end_date,
    FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` css
    WHERE css.state_code = 'US_IX'
),

assessments AS (
    -- Grouping dedupes dates across sources so a same-day assessment and LSI-R-review
    -- contact don't break the LEAD-based sequencing below. A date from several
    -- sources keeps the longest lookback any of them allows.
    SELECT
        state_code,
        person_id,
        assessment_date,
        MAX(lookback_days) AS lookback_days,
    FROM (
        SELECT
            state_code,
            person_id,
            assessment_date,
            {assessment_lookback_days} AS lookback_days,
        FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
        WHERE state_code = 'US_IX'
            AND assessment_type = '{state_assessment_type}'
            AND assessment_class = '{assessment_class}'
{supervision_contact_union}
{psi_lsir_union}
    )
    GROUP BY state_code, person_id, assessment_date
),

critical_date_spans AS (
    SELECT *
    FROM (
        SELECT
            ss.state_code,
            ss.person_id,
            -- Clamp to session start so a pre-session assessment doesn't start the span
            -- before supervision begins (critical date still uses the true assessment date).
            GREATEST(
                IFNULL(a.assessment_date, ss.start_date),
                ss.start_date
            ) AS start_datetime,
            -- The end_datetime is either the next assessment date (if it exists) or the end of the
            -- supervision session.
            IF(
                a.assessment_date IS NOT NULL,
                -- next_assessment date OR end_date, whichever is earlier
                LEAST(
                    IFNULL(
                        LEAD(a.assessment_date) OVER (
                            PARTITION BY ss.state_code, ss.person_id
                            ORDER BY a.assessment_date
                        ),
                        '9999-12-31'
                    ),
                    {nonnull_end_date_clause('ss.end_date')}
                ),
                ss.end_date
            ) AS end_datetime,
            DATE_ADD(a.assessment_date, INTERVAL 365 DAY) AS critical_date,
            a.assessment_date AS last_assessment_date,
        FROM supervision_sessions ss
        LEFT JOIN assessments a
            ON ss.person_id = a.person_id
                AND ss.state_code = a.state_code
                -- Reach back up to the assessment's lookback before session start,
                -- but not past the previous session's end.
                AND a.assessment_date BETWEEN GREATEST(
                        DATE_SUB(ss.start_date, INTERVAL a.lookback_days DAY),
                        {nonnull_start_date_clause('ss.prev_session_end_date')}
                    )
                    AND {nonnull_end_date_exclusive_clause('ss.end_date')}
    )
    -- Drop spans collapsed to zero/negative length by the start clamp (a pre-session
    -- assessment superseded by an in-session one); its period is already covered.
    WHERE start_datetime < {nonnull_end_date_exclusive_clause('end_datetime')}
),

{critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_time=30,
    attributes=['last_assessment_date'])}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(
        STRUCT(
            critical_date AS assessment_due_date,
            last_assessment_date AS last_assessment_date,
            critical_date_has_passed AS is_missing_annual_assessment,
            '1 EVERY 365 DAYS' AS contact_cadence,
            '{state_assessment_type}' AS assessment_type,
            '{assessment_class}' AS assessment_class
    )) AS reason,
    critical_date AS assessment_due_date,
    last_assessment_date,
    critical_date_has_passed AS is_missing_annual_assessment,
    '1 EVERY 365 DAYS' AS contact_cadence,
    '{state_assessment_type}' AS assessment_type,
    '{assessment_class}' AS assessment_class,
FROM critical_date_has_passed_spans
"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        assessment_type=assessment_type,
        reasons_fields=[
            ReasonsField(
                name="assessment_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Due date of the assessment.",
            ),
            ReasonsField(
                name="last_assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last assessment.",
            ),
            ReasonsField(
                name="is_missing_annual_assessment",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether the client is missing an annual assessment.",
            ),
            ReasonsField(
                name="contact_cadence",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The required cadence for assessments.",
            ),
            ReasonsField(
                name="assessment_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of assessment.",
            ),
            ReasonsField(
                name="assessment_class",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The class of assessment.",
            ),
        ],
    )


def release_district_case_notes() -> str:
    """Returns the case notes that show where a resident's release district comes
    from. A resident with CRC Request notes in the current incarceration gets those
    notes. Otherwise they get one note explaining the district from their home address
    or, without an Idaho one, from the county of their latest sentence, matching
    release_district_source in us_ix_release_district_spans.

    For example, a resident with a CRC Request note this incarceration gets the first
    row below, and one with no such note but an Ada County home address gets the
    second (columns abridged):

        note_title,note_body
        Release district from CRC Request note: DISTRICT 3,"CRC Request - D3 Release: Plans to live with family in Nampa."
        Release district from home address: DISTRICT 4,"No CRC Request note this incarceration. Home address on file: 123 MAIN ST, BOISE ID 83702 (Ada County), which is in DISTRICT 4."
    """
    return f"""
        SELECT
            crc_notes.external_id,
            crc_notes.criteria,
            CONCAT(
                'Release district from CRC Request note: ',
                {release_district_from_crc_request_note_title("crc_notes.note_title")}
            ) AS note_title,
            CONCAT(crc_notes.note_title, ': ', IFNULL(crc_notes.note_body, '')) AS note_body,
            crc_notes.event_date,
        FROM (
{ix_general_case_notes(
    where_clause_addition=f"AND ContactModeDesc IN {RELEASE_INFORMATION_CONTACT_MODES}",
    criteria_str=RELEASE_INFORMATION_STR,
    indent_level=12,
)}
        ) crc_notes
        INNER JOIN `{{project_id}}.reference_views.product_stable_person_external_ids_materialized` pei
            ON crc_notes.external_id = pei.stable_person_external_id
            AND pei.state_code = 'US_IX'
            AND pei.system_type = 'INCARCERATION'
        INNER JOIN `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` sess
            ON pei.person_id = sess.person_id
            AND sess.compartment_level_1 = 'INCARCERATION'
            AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="sess.start_date",
                end_date_column="sess.end_date_exclusive",
            )}
        WHERE crc_notes.event_date >= sess.start_date

        UNION ALL

        SELECT
            pei.stable_person_external_id AS external_id,
            '{RELEASE_INFORMATION_STR}' AS criteria,
            CASE rds.{RELEASE_DISTRICT_SOURCE_COLUMN_NAME}
                WHEN '{UsIxReleaseDistrictSource.ADDRESS.value}' THEN CONCAT(
                    'Release district from home address: ',
                    rds.{ADDRESS_RELEASE_DISTRICT_COLUMN_NAME}
                )
                WHEN '{UsIxReleaseDistrictSource.SENTENCING_COUNTY.value}' THEN CONCAT(
                    'Release district from sentencing county: ',
                    rds.{SENTENCING_COUNTY_RELEASE_DISTRICT_COLUMN_NAME}
                )
            END AS note_title,
            CASE rds.{RELEASE_DISTRICT_SOURCE_COLUMN_NAME}
                WHEN '{UsIxReleaseDistrictSource.ADDRESS.value}' THEN CONCAT(
                    'No CRC Request note this incarceration. Home address on file: ',
                    rds.{PHYSICAL_ADDRESS_COLUMN_NAME},
                    ', which is in ',
                    rds.{ADDRESS_RELEASE_DISTRICT_COLUMN_NAME},
                    '.'
                )
                WHEN '{UsIxReleaseDistrictSource.SENTENCING_COUNTY.value}' THEN CONCAT(
                    'No CRC Request note this incarceration and no Idaho home address ',
                    'on file. Most recent sentence was imposed in ',
                    INITCAP(rds.{SENTENCING_COUNTY_NAME_COLUMN_NAME}),
                    ' County, which is in ',
                    rds.{SENTENCING_COUNTY_RELEASE_DISTRICT_COLUMN_NAME},
                    '.'
                )
            END AS note_body,
            rds.start_date AS event_date,
        FROM `{{project_id}}.{ANALYST_VIEWS_DATASET}.{US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME}_materialized` rds
        INNER JOIN `{{project_id}}.reference_views.product_stable_person_external_ids_materialized` pei
            ON rds.person_id = pei.person_id
            AND pei.state_code = 'US_IX'
            AND pei.system_type = 'INCARCERATION'
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="rds.start_date",
                end_date_column="rds.end_date_exclusive",
            )}
            AND rds.{RELEASE_DISTRICT_SOURCE_COLUMN_NAME} IN (
                '{UsIxReleaseDistrictSource.ADDRESS.value}',
                '{UsIxReleaseDistrictSource.SENTENCING_COUNTY.value}'
            )"""


def crc_case_notes_cte(
    crc_denied_months: int = 6,
    include_work_history: bool = False,
) -> str:
    """
    Returns a SQL query string for case notes relevant to CRC (Community
    Reentry Center) eligibility in Idaho, with all queries joined by UNION ALL.

    Args:
        crc_denied_months: Number of months to look back for CRC denied case notes.
            Defaults to 6 months.
        include_work_history: Whether to include work history case notes.
            Defaults to False.

    The case notes include:
    - Offender alerts (excluding victims)
    - Institutional behavior notes (corrective action and positive)
    - Release information
    - CRC information and denial notes
    - I-9 documents
    - Work history (optional)
    - Medical clearance
    - Violent charges being served
    - NCIC/ILETS checks
    - Escape, absconsion, or eluding police history
    - Detainers
    - DORs (Disciplinary Offense Reports)
    - Program enrollment
    - Victim alerts
    """
    union_separator = "\n\n        UNION ALL\n\n"

    queries = [
        # Offender alerts (excluding victims)
        "--Offender alerts (excluding victims)\n"
        + ix_offender_alerts_case_notes(where_clause="WHERE AlertId != '133'"),
        # Institutional Behavior Notes - Corrective Action
        "        --Institutional Behavior Notes - Corrective Action\n"
        + ix_general_case_notes(
            where_clause_addition="AND ContactModeDesc = 'Corrective Action'",
            criteria_str=INSTITUTIONAL_BEHAVIOR_NOTES_STR,
            in_the_past_x_months=6,
        ),
        # Positive behavior notes
        "        --Positive [behavior notes]\n"
        + ix_general_case_notes(
            where_clause_addition="AND ContactModeDesc = 'Positive'",
            criteria_str=INSTITUTIONAL_BEHAVIOR_NOTES_STR,
            in_the_past_x_months=6,
        ),
        "        --Release district information\n" + release_district_case_notes(),
        # Additional CRC info (in the past 6 months)
        "        --Additional CRC info (in the past 6 months)\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND ContactModeDesc IN {CRC_INFORMATION_CONTACT_MODES}",
            criteria_str=CRC_INFORMATION_STR,
            in_the_past_x_months=6,
        ),
        # CRC denied case notes
        f"        --CRC denied case notes (in the past {crc_denied_months} months)\n"
        + ix_general_case_notes(
            where_clause_addition="AND ContactModeDesc = 'CRC Termer Denied'",
            criteria_str="CRC Denial Case Notes",
            in_the_past_x_months=crc_denied_months,
        ),
        # I-9 Documents
        "        --I-9 Documents\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND REGEXP_CONTAINS(UPPER(note.Details), r'{I9_NOTE_TX_REGEX}')",
            criteria_str=I9_NOTES_STR,
            in_the_past_x_months=60,
        ),
        # Medical clearance
        "        --Medical clearance\n"
        + ix_general_case_notes(
            where_clause_addition=f"AND REGEXP_CONTAINS(UPPER(note.Details), r'{MEDICAL_CLEARANCE_TX_REGEX}')",
            criteria_str=MEDICAL_CLEARANCE_STR,
            in_the_past_x_months=6,
        ),
        # Violent charges being served
        "        --Violent charges being served\n"
        + "("
        + current_violent_statutes_being_served(state_code="US_IX")
        + ")",
        # NCIC/ILETS
        "        --NCIC/ILETS\n"
        + ix_fuzzy_matched_case_notes(where_clause="WHERE ncic_ilets_nco_check"),
        # Recent escape, absconsion or eluding police
        "        --Recent escape, absconsion or eluding police\n"
        + escape_absconsion_or_eluding_police_case_notes(),
        # Detainers
        "        --Detainers\n" + detainer_case_notes(),
        # DORs
        "        --DORs\n"
        + dor_case_notes_query(in_the_past_x_months=6, indent_level=8),
        # Program Enrollment
        "        --Program Enrollment\n" + program_enrollment_query(),
        # Victim alerts
        "        --Victim alerts\n" + victim_alert_notes(),
    ]

    # Work history (optional, for resident worker)
    if include_work_history:
        queries.append(
            "        --Work History\n"
            + ix_general_case_notes(
                where_clause_addition="AND ContactModeDesc = 'Work History'",
                criteria_str=WORK_HISTORY_STR,
                in_the_past_x_months=60,
            )
        )

    return union_separator.join(queries)


def _release_district_criteria_merge_column_names(
    *, release_district_column_name: str
) -> list[str]:
    """Returns the us_ix_release_district_spans columns whose changes start a new
    criteria span when the criteria reads release_district_column_name."""
    if release_district_column_name == CRC_REQUEST_RELEASE_DISTRICT_COLUMN_NAME:
        # Keeps consecutive notes naming the same district as separate spans.
        return [
            CRC_REQUEST_RELEASE_DISTRICT_COLUMN_NAME,
            CRC_REQUEST_NOTE_DATE_COLUMN_NAME,
        ]
    if release_district_column_name == RELEASE_DISTRICT_COLUMN_NAME:
        return [RELEASE_DISTRICT_COLUMN_NAME]
    raise ValueError(
        f"Unexpected release_district_column_name [{release_district_column_name}]"
    )


def release_district_criteria_query(
    *,
    release_districts: list[UsIxReleaseDistrict],
    release_district_column_name: str,
) -> str:
    """Returns a criteria query over us_ix_release_district_spans that meets the
    criteria while the district in release_district_column_name is one of
    release_districts, with no span while that column is NULL.

    For example, with release_districts [DISTRICT_3, DISTRICT_4] and
    release_district_column_name crc_request_release_district, a resident with a D4
    note on 2025-06-03 and an ISC note on 2025-09-15 gets (columns abridged):

        start_date,end_date,meets_criteria,release_district
        2025-06-03,2025-09-15,true,4
        2025-09-15,,false,ISC

    Args:
        release_districts: The districts that meet the criteria.
        release_district_column_name: The view column to read, either
            crc_request_release_district (one span per CRC Request note) or
            release_district (the CRC note, home address or sentencing county
            district, whichever is set first).
    """
    source_column_names = _release_district_criteria_merge_column_names(
        release_district_column_name=release_district_column_name
    )
    release_districts_str = list_to_query_string(
        [district.value for district in release_districts],
        quoted=True,
        single_quote=True,
    )

    # Columns this criteria doesn't read split the view's spans, so the spans are
    # merged back on the columns it does read.
    merged_spans_query = aggregate_adjacent_spans(
        table_name="release_district_spans",
        attribute=[*source_column_names, "release_district_code"],
        end_date_field_name="end_date_exclusive",
    )

    return f"""
WITH release_district_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        {list_to_query_string(source_column_names)},
        -- Turns DISTRICT 3 into 3 and leaves ISC as is.
        REGEXP_EXTRACT({release_district_column_name}, r'ISC|\\d+') AS release_district_code,
    FROM `{{project_id}}.{ANALYST_VIEWS_DATASET}.{US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME}_materialized`
    WHERE {release_district_column_name} IS NOT NULL
),
merged_release_district_spans AS (
{fix_indent(merged_spans_query, indent_level=4)}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    {release_district_column_name} IN ({release_districts_str}) AS meets_criteria,
    TO_JSON(STRUCT(release_district_code AS release_district)) AS reason,
    release_district_code AS release_district,
FROM merged_release_district_spans
"""


# Days after case assignment within which residence and employment must be verified.
US_IX_INITIAL_VERIFICATION_DAYS = 30

# Spans from each person's start of supervision to the end of their supervision super
# session, excluding Expanded CRC Furlough (XCRC) clients, whom policy exempts from the
# initial residence and employment verifications. The start is the first COURT_SENTENCE
# or RELEASE_FROM_INCARCERATION start in the super session, since those reasons mark a
# person beginning supervision rather than continuing it; without one, the super session
# start is used.
_US_IX_INITIAL_VERIFICATION_PERIOD_SPANS_QUERY = f"""
    WITH supervision_starts AS (
        SELECT
            sss.state_code,
            sss.person_id,
            MIN(IFNULL(cl1.start_date, sss.start_date)) AS start_date,
            sss.end_date_exclusive AS end_date,
        FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
        LEFT JOIN `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` cl1
            ON cl1.person_id = sss.person_id
            AND cl1.state_code = sss.state_code
            AND cl1.start_date BETWEEN sss.start_date AND {nonnull_end_date_clause('sss.end_date')}
            AND cl1.start_reason IN ('COURT_SENTENCE', 'RELEASE_FROM_INCARCERATION')
            AND cl1.compartment_level_1 = 'SUPERVISION'
        WHERE sss.state_code = 'US_IX'
        GROUP BY sss.state_code, sss.person_id, sss.supervision_super_session_id, sss.end_date_exclusive
    )
    SELECT
        ss.state_code,
        ss.person_id,
        ss.start_date,
        ss.end_date,
    FROM supervision_starts ss
    LEFT JOIN `{{project_id}}.tasks_views.us_ix_case_type_supervision_level_spans_materialized` xcrc
        ON xcrc.person_id = ss.person_id
        AND xcrc.state_code = ss.state_code
        AND xcrc.case_type = 'XCRC'
        AND ss.start_date BETWEEN xcrc.start_date AND {nonnull_end_date_clause('xcrc.end_date')}
    WHERE xcrc.person_id IS NULL
"""


def us_ix_initial_verification_trigger_view_builder(
    *,
    criteria_name: str,
    description: str,
    contact_type: ContactType,
    only_include_completed_contacts: bool,
    include_earliest_unmet_due_date: bool,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a criterion view builder for a one-time verification due within
    US_IX_INITIAL_VERIFICATION_DAYS of the start of supervision. Each span is open
    until a contact of contact_type is logged. XCRC clients get no spans.

    Args:
        criteria_name (str): The name of the criterion view.
        description (str): A brief description of the criterion view.
        contact_type (ContactType): The contact type that satisfies the verification.
        only_include_completed_contacts (bool): Whether only COMPLETED contacts
            satisfy the verification.
        include_earliest_unmet_due_date (bool): Whether to emit an
            earliest_unmet_due_date reasons field, for criteria grouped with a
            fixed-cadence criterion that surfaces that field as the display due date.
    """
    return non_recurring_contact_compliance_builder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_IX,
        contact_events_query=get_contact_events_for_type(
            [contact_type],
            StateCode.US_IX,
            only_include_completed_contacts=only_include_completed_contacts,
        ),
        contact_types=[contact_type],
        contact_period_spans_query=_US_IX_INITIAL_VERIFICATION_PERIOD_SPANS_QUERY,
        time_period=US_IX_INITIAL_VERIFICATION_DAYS,
        only_include_completed_contacts=only_include_completed_contacts,
        include_earliest_unmet_due_date=include_earliest_unmet_due_date,
    )
