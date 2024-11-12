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
"""Interface for working with the AgencySetting model."""


from typing import Any, List, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.justice_counts import schema


class AgencySettingInterface:
    """Contains methods for setting and getting AgencySettings."""

    @staticmethod
    def create_or_update_agency_setting(
        session: Session,
        agency_id: int,
        setting_type: schema.AgencySettingType,
        value: Any,
    ) -> None:
        insert_statement = insert(schema.AgencySetting).values(
            source_id=agency_id, setting_type=setting_type.value, value=value
        )
        insert_statement = insert_statement.on_conflict_do_update(
            constraint="unique_agency_setting",
            set_={"value": value},
        )

        session.execute(insert_statement)

    @staticmethod
    def get_agency_settings(
        session: Session,
        agency_id: int,
    ) -> List[schema.AgencySetting]:
        return (
            session.query(schema.AgencySetting)
            .filter(schema.AgencySetting.source_id == agency_id)
            .all()
        )

    @staticmethod
    def get_agency_setting_by_setting_type(
        session: Session,
        agency_id: int,
        setting_type: schema.AgencySettingType,
    ) -> Optional[schema.AgencySetting]:
        return (
            session.query(schema.AgencySetting)
            .filter(
                schema.AgencySetting.source_id == agency_id,
                schema.AgencySetting.setting_type == setting_type.value,
            )
            .one_or_none()
        )
