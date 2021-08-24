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
""" Interface for interacting with OfficerMetadata records """

import sqlalchemy.orm.exc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.case_triage.schema import OfficerMetadata


class OfficerMetadataInterface:
    """ Utility methods for interacting with `OfficerMetadata` records """

    @classmethod
    def get_officer_metadata(
        cls, session: Session, state_code: str, officer_external_id: str
    ) -> OfficerMetadata:
        """ Fetches an `OfficerMetadata` record """
        default_metadata = OfficerMetadata(
            state_code=state_code,
            officer_external_id=officer_external_id,
            has_seen_onboarding=False,
        )

        try:
            return (
                session.query(OfficerMetadata)
                .filter(
                    OfficerMetadata.officer_external_id == officer_external_id,
                    OfficerMetadata.state_code == state_code,
                )
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound:
            return default_metadata

    @classmethod
    def set_has_seen_onboarding(
        cls,
        session: Session,
        state_code: str,
        officer_external_id: str,
        has_seen_onboarding: bool,
    ) -> None:
        """ Upserts the `OfficerMetadata` record """
        values = {"has_seen_onboarding": has_seen_onboarding}
        insert_statement = (
            insert(OfficerMetadata)
            .values(
                state_code=state_code,
                officer_external_id=officer_external_id,
                **values,
            )
            .on_conflict_do_update(
                constraint="officer_metadata_pkey", set_=dict(**values)
            )
        )

        session.execute(insert_statement)
        session.commit()
