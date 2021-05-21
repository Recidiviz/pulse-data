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
"""Interface for setting ClientInfo columns."""
from typing import Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.demo_helpers import (
    fake_person_id_for_demo_user,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    ClientInfo,
    ETLClient,
)


class ClientInfoInterface:
    """Contains methods for setting and getting preferred client info."""

    @staticmethod
    def set_preferred_contact_method(
        session: Session, client: ETLClient, contact_method: PreferredContactMethod
    ) -> None:
        insert_statement = (
            insert(ClientInfo)
            .values(
                person_external_id=client.person_external_id,
                state_code=client.state_code,
                preferred_contact_method=contact_method.value,
            )
            .on_conflict_do_update(
                constraint="unique_person",
                set_={"preferred_contact_method": contact_method.value},
            )
        )
        session.execute(insert_statement)
        session.commit()

    @staticmethod
    def set_preferred_name(
        session: Session, client: ETLClient, name: Optional[str]
    ) -> None:
        insert_statement = (
            insert(ClientInfo)
            .values(
                person_external_id=client.person_external_id,
                state_code=client.state_code,
                preferred_name=name,
            )
            .on_conflict_do_update(
                constraint="unique_person",
                set_={"preferred_name": name},
            )
        )
        session.execute(insert_statement)
        session.commit()


class DemoClientInfoInterface:
    """Contains methods for setting and getting preferred client info for demo users."""

    @staticmethod
    def set_preferred_contact_method(
        session: Session,
        user_email: str,
        client: ETLClient,
        contact_method: PreferredContactMethod,
    ) -> None:
        insert_statement = (
            insert(ClientInfo)
            .values(
                person_external_id=fake_person_id_for_demo_user(
                    user_email, client.person_external_id
                ),
                state_code=client.state_code,
                preferred_contact_method=contact_method.value,
            )
            .on_conflict_do_update(
                constraint="unique_person",
                set_={"preferred_contact_method": contact_method.value},
            )
        )
        session.execute(insert_statement)
        session.commit()

    @staticmethod
    def set_preferred_name(
        session: Session, user_email: str, client: ETLClient, name: Optional[str]
    ) -> ClientInfo:
        fake_person_external_id = fake_person_id_for_demo_user(
            user_email, client.person_external_id
        )
        insert_statement = (
            insert(ClientInfo)
            .values(
                person_external_id=fake_person_external_id,
                state_code=client.state_code,
                preferred_name=name,
            )
            .on_conflict_do_update(
                constraint="unique_person",
                set_={"preferred_name": name},
            )
        )
        session.execute(insert_statement)
        session.commit()

        return (
            session.query(ClientInfo)
            .filter(
                ClientInfo.person_external_id == fake_person_external_id,
                ClientInfo.state_code == client.state_code,
            )
            .one()
        )
