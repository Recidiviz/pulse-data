# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Interface for working with the AgencyJurisdiction model."""
from typing import Dict, List, Optional

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyJurisdictionInterface:
    """Contains methods for setting and getting AgencyJurisdictions."""

    @staticmethod
    def create_or_update_agency_jurisdictions(
        session: Session,
        agency_id: int,
        included_jurisdiction_ids: List[str],
        excluded_jurisdiction_ids: List[str],
    ) -> None:
        """For the specified agency, deletes all rows in the AgencyJurisdiction table.
        Then, inserts the updated rows in the AgencyJurisdiction table.
        """
        # first, delete all existing jurisdiction records for the agency
        delete_statement = delete(schema.AgencyJurisdiction).where(
            schema.AgencyJurisdiction.source_id == agency_id
        )
        session.execute(delete_statement)

        # now, insert new included rows
        for jurisdiction_id in included_jurisdiction_ids:
            insert_statement = insert(schema.AgencyJurisdiction).values(
                source_id=agency_id,
                membership=schema.AgencyJurisdictionType.INCLUDE.value,
                jurisdiction_id=jurisdiction_id,
            )
            session.execute(insert_statement)
        # now, insert new excluded rows
        for jurisdiction_id in excluded_jurisdiction_ids:
            insert_statement = insert(schema.AgencyJurisdiction).values(
                source_id=agency_id,
                membership=schema.AgencyJurisdictionType.EXCLUDE.value,
                jurisdiction_id=jurisdiction_id,
            )
            session.execute(insert_statement)

    @staticmethod
    def to_json(
        session: Session,
        agency_id: int,
        is_v2: bool = False,
        fips_code_to_geoid: Optional[Dict[str, str]] = None,
    ) -> Dict:
        all_agency_jurisdictions = (
            session.query(schema.AgencyJurisdiction).filter(
                schema.AgencyJurisdiction.source_id == agency_id
            )
        ).all()

        included_ids = []
        excluded_ids = []
        for jurisdiction in all_agency_jurisdictions:
            if jurisdiction.membership == "INCLUDE":
                included_ids.append(jurisdiction.jurisdiction_id)
            else:
                excluded_ids.append(jurisdiction.jurisdiction_id)

        if is_v2 is True and fips_code_to_geoid is not None:
            included_ids = [
                fips_code_to_geoid.get(id.rstrip("0")) for id in included_ids
            ]
            excluded_ids = [
                fips_code_to_geoid.get(id.rstrip("0")) for id in excluded_ids
            ]

        return {
            "included": included_ids,
            "excluded": excluded_ids,
        }
