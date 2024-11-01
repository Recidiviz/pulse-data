# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""This class implements tests for the Justice Counts AgencySettingInterface."""

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import JusticeCountsDatabaseTestCase


class TestAgencySettingInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the AgencySettingInterface."""

    def setUp(self) -> None:
        super().setUp()
        with SessionFactory.using_database(self.database_key) as session:
            agency_A = AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Alpha",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            agency_B = AgencyInterface.create_or_update_agency(
                session=session,
                name="Beta Initiative",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            session.commit()
            session.refresh(agency_A)
            session.refresh(agency_B)
            self.agency_A_id = agency_A.id
            self.agency_B_id = agency_B.id

    def test_create_and_get_agency_setting(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            AgencySettingInterface.create_or_update_agency_setting(
                session=session,
                agency_id=self.agency_A_id,
                setting_type=schema.AgencySettingType.TEST,
                value=["foo", "bar"],
            )
            AgencySettingInterface.create_or_update_agency_setting(
                session=session,
                agency_id=self.agency_B_id,
                setting_type=schema.AgencySettingType.TEST,
                value=[],
            )

        with SessionFactory.using_database(self.database_key) as session:
            agency_A_settings = AgencySettingInterface.get_agency_settings(
                session=session, agency_id=self.agency_A_id
            )
            agency_B_settings = AgencySettingInterface.get_agency_settings(
                session=session, agency_id=self.agency_B_id
            )
            self.assertEqual(len(agency_A_settings), 1)
            self.assertEqual(len(agency_B_settings), 1)
            self.assertEqual(
                agency_A_settings[0].setting_type,
                schema.AgencySettingType.TEST.value,
            )
            self.assertEqual(agency_A_settings[0].value, ["foo", "bar"])
            self.assertEqual(
                agency_B_settings[0].setting_type,
                schema.AgencySettingType.TEST.value,
            )
            self.assertEqual(agency_B_settings[0].value, [])

        with SessionFactory.using_database(self.database_key) as session:
            # Update existing setting
            AgencySettingInterface.create_or_update_agency_setting(
                session=session,
                agency_id=self.agency_A_id,
                setting_type=schema.AgencySettingType.TEST,
                value="new value",
            )

        with SessionFactory.using_database(self.database_key) as session:
            agency_A_settings = AgencySettingInterface.get_agency_settings(
                session=session, agency_id=self.agency_A_id
            )
            self.assertEqual(len(agency_A_settings), 1)
            self.assertEqual(agency_A_settings[0].value, "new value")
