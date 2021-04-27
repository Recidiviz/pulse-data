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
"""Defines the types needed for modeling CaseUpdates."""
from datetime import date
from enum import Enum
from typing import Dict, Optional, Any

import attr
import dateutil.parser

from recidiviz.persistence.database.schema.case_triage.schema import ETLClient


class CaseUpdateActionType(Enum):
    # Risk assessment
    COMPLETED_ASSESSMENT = "COMPLETED_ASSESSMENT"
    INCORRECT_ASSESSMENT_DATA = "INCORRECT_ASSESSMENT_DATA"
    # Employment
    FOUND_EMPLOYMENT = "FOUND_EMPLOYMENT"
    INCORRECT_EMPLOYMENT_DATA = "INCORRECT_EMPLOYMENT_DATA"
    # Face to face contact
    SCHEDULED_FACE_TO_FACE = "SCHEDULED_FACE_TO_FACE"
    INCORRECT_CONTACT_DATA = "INCORRECT_CONTACT_DATA"

    DISCHARGE_INITIATED = "DISCHARGE_INITIATED"
    DOWNGRADE_INITIATED = "DOWNGRADE_INITIATED"

    NOT_ON_CASELOAD = "NOT_ON_CASELOAD"
    CURRENTLY_IN_CUSTODY = "CURRENTLY_IN_CUSTODY"

    DEPRECATED__INFORMATION_DOESNT_MATCH_OMS = "INFORMATION_DOESNT_MATCH_OMS"
    DEPRECATED__FILED_REVOCATION_OR_VIOLATION = "FILED_REVOCATION_OR_VIOLATION"
    DEPRECATED__OTHER_DISMISSAL = "OTHER_DISMISSAL"


class CaseUpdateMetadataKeys:
    LAST_EMPLOYER = "last_employer"
    LAST_RECORDED_DATE = "last_recorded_date"
    LAST_SUPERVISION_LEVEL = "last_supervision_level"


@attr.s
class CaseActionVersionData:
    """ Contains logic for denormalizing and serializing a version of client data """

    last_employer: Optional[str] = attr.ib(default=None)
    last_recorded_date: Optional[date] = attr.ib(default=None)
    last_supervision_level: Optional[str] = attr.ib(default=None)

    def to_json(self) -> Dict[str, str]:
        base = {}
        if self.last_recorded_date is not None:
            base[
                CaseUpdateMetadataKeys.LAST_RECORDED_DATE
            ] = self.last_recorded_date.isoformat()
        if self.last_supervision_level is not None:
            base[
                CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL
            ] = self.last_supervision_level

        if self.last_employer is not None:
            base[CaseUpdateMetadataKeys.LAST_EMPLOYER] = self.last_employer

        return base

    @staticmethod
    def from_json(json_dict: Dict[str, str]) -> "CaseActionVersionData":
        kwargs: Dict[str, Any] = {**json_dict}

        if CaseUpdateMetadataKeys.LAST_RECORDED_DATE in json_dict:
            kwargs[CaseUpdateMetadataKeys.LAST_RECORDED_DATE] = dateutil.parser.parse(
                json_dict[CaseUpdateMetadataKeys.LAST_RECORDED_DATE]
            ).date()

        return CaseActionVersionData(**kwargs)

    @staticmethod
    def from_client_mappings(
        mappings: Dict[str, str],
        client: ETLClient,
    ) -> "CaseActionVersionData":
        if not all(hasattr(ETLClient, column) for column in mappings.values()):
            raise ValueError(
                f"Some mapping columns were not found in ETLClient. mappings: {mappings}"
            )

        return CaseActionVersionData(
            **{
                metadata_key: getattr(client, column_name)
                for metadata_key, column_name in mappings.items()
            }
        )
