# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Commonly used objects for working with fixture data"""


from datetime import date
from typing import Any, Dict, List, Literal, Optional, Union

import dateutil.parser

FixtureType = Union[
    Literal["clients"], Literal["opportunities"], Literal["client_events"]
]


def parse_nullable_date(date_str: str) -> Optional[date]:
    if not date_str:
        return None
    return dateutil.parser.parse(date_str).date()


def treat_empty_as_null(input_str: str) -> Optional[str]:
    if not input_str:
        return None
    return input_str


def csv_row_to_etl_client_json(row: List[str]) -> Dict[str, Any]:
    return {
        "person_external_id": row[1],
        "state_code": row[8],
        "supervising_officer_external_id": row[0],
        "full_name": treat_empty_as_null(row[2]),
        "gender": treat_empty_as_null(row[12]),
        "current_address": treat_empty_as_null(row[3]),
        "birthdate": parse_nullable_date(row[4]),
        "supervision_start_date": parse_nullable_date(row[13]),
        "projected_end_date": parse_nullable_date(row[15]),
        "supervision_type": row[5],
        "case_type": row[6],
        "supervision_level": row[7],
        "employer": treat_empty_as_null(row[9]),
        "last_known_date_of_employment": parse_nullable_date(row[16]),
        "most_recent_assessment_date": parse_nullable_date(row[10]),
        "assessment_score": int(row[14]),
        "most_recent_face_to_face_date": parse_nullable_date(row[11]),
        "most_recent_home_visit_date": parse_nullable_date(row[17]),
        "days_with_current_po": int(row[18]),
        "email_address": treat_empty_as_null(row[19]),
        "days_on_current_supervision_level": int(row[20]),
        "phone_number": treat_empty_as_null(row[21]),
        "exported_at": parse_nullable_date(row[22]),
        "next_recommended_assessment_date": parse_nullable_date(row[23]),
        "employment_start_date": parse_nullable_date(row[24]),
        "most_recent_violation_date": parse_nullable_date(row[25]),
        "next_recommended_face_to_face_date": parse_nullable_date(row[26]),
        "next_recommended_home_visit_date": parse_nullable_date(row[27]),
        "most_recent_treatment_collateral_contact_date": parse_nullable_date(row[28]),
        "next_recommended_treatment_collateral_contact_date": parse_nullable_date(
            row[29]
        ),
    }
