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
"""Implements the Querier abstraction that is responsible for considering multiple
data sources and coalescing answers for downstream consumers."""
from typing import List

import sqlalchemy.orm.exc
from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.case_triage.schema import ETLClient, ETLOfficer


class PersonDoesNotExistError(ValueError):
    pass


class CaseTriageQuerier:
    """Implements Querier abstraction for Case Triage data sources."""

    @staticmethod
    def clients_for_officer(session: Session, officer: ETLOfficer) -> List[ETLClient]:
        return session.query(ETLClient).filter_by(
            supervising_officer_external_id=officer.external_id,
            state_code=officer.state_code,
        ).all()

    @staticmethod
    def client_with_id_and_state_code(session: Session,
                                      person_external_id: str,
                                      state_code: str) -> ETLClient:
        try:
            return session.query(ETLClient).filter_by(
                person_external_id=person_external_id,
                state_code=state_code,
            ).one()
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise PersonDoesNotExistError(f'could not find id: {person_external_id}') from e

    @staticmethod
    def officer_for_email(session: Session, officer_email: str) -> ETLOfficer:
        email = officer_email.lower()
        return session.query(ETLOfficer).filter_by(email_address=email).one()
