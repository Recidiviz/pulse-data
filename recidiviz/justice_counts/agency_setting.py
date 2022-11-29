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


import enum
from typing import Any, List

from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.justice_counts import schema


class AgencySettingType(enum.Enum):
    TEST = "TEST"
    PURPOSE_AND_FUNCTIONS = "PURPOSE_AND_FUNCTIONS"


class AgencySettingInterface:
    """Contains methods for setting and getting AgencySettings."""

    @staticmethod
    def create_agency_setting(
        session: Session,
        agency_id: int,
        setting_type: AgencySettingType,
        value: Any,
    ) -> schema.AgencySetting:
        agency_setting = schema.AgencySetting(
            source_id=agency_id,
            setting_type=setting_type.value,
            value=value,
        )
        session.add(agency_setting)
        session.commit()
        return agency_setting

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
