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
"""This class implements tests for the Justice Counts AgencyInterface."""

from datetime import datetime, timezone

from freezegun import freeze_time

from recidiviz.common.fips import (
    get_county_code_to_county_fips,
    get_county_code_to_county_name,
)
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.dimensions.courts import (
    FundingType,
    SentenceType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.geoid import get_fips_code_to_geoid
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import JusticeCountsDatabaseTestCase


class TestAgencyInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the AgencyInterface."""

    def setUp(self) -> None:
        super().setUp()
        with SessionFactory.using_database(self.database_key) as session:
            AgencyInterface.create_or_update_agency(
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
            AgencyInterface.create_or_update_agency(
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

    def test_get_agencies(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agencyAlphaByNameStateSystem = (
                AgencyInterface.get_agency_by_name_state_and_systems(
                    session=session,
                    name="Agency Alpha",
                    state_code="us_ca",
                    systems=[schema.System.LAW_ENFORCEMENT.value],
                )
            )
            agencyBetaByNameStateSystem = (
                AgencyInterface.get_agency_by_name_state_and_systems(
                    session=session,
                    name="Beta Initiative",
                    state_code="us_ak",
                    systems=[schema.System.LAW_ENFORCEMENT.value],
                )
            )

            AgencyInterface.create_or_update_agency(
                session=session,
                name="2 System Initiative",
                systems=[schema.System.LAW_ENFORCEMENT, schema.System.PRISONS],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )

            twoSystemsByNameStateSystem = (
                AgencyInterface.get_agency_by_name_state_and_systems(
                    session=session,
                    name="2 System Initiative",
                    state_code="us_ak",
                    systems=[
                        schema.System.LAW_ENFORCEMENT.value,
                        schema.System.PRISONS.value,
                    ],
                )
            )

            twoSystemsByNameStateSystemDifferentOrder = (
                AgencyInterface.get_agency_by_name_state_and_systems(
                    session=session,
                    name="2 System Initiative",
                    state_code="us_ak",
                    systems=[
                        schema.System.PRISONS.value,
                        schema.System.LAW_ENFORCEMENT.value,
                    ],
                )
            )

            self.assertEqual(agencyAlphaByNameStateSystem.name, "Agency Alpha")
            self.assertEqual(agencyBetaByNameStateSystem.name, "Beta Initiative")
            self.assertEqual(twoSystemsByNameStateSystem.name, "2 System Initiative")
            self.assertEqual(
                twoSystemsByNameStateSystemDifferentOrder.name, "2 System Initiative"
            )

            allAgencies = AgencyInterface.get_agencies(session=session)
            self.assertEqual(
                {a.name for a in allAgencies},
                {"Agency Alpha", "Beta Initiative", "2 System Initiative"},
            )

            allAgencyIds = AgencyInterface.get_agency_ids(session=session)
            self.assertEqual(sorted(allAgencyIds), sorted(a.id for a in allAgencies))

            agenciesById = AgencyInterface.get_agencies_by_id(
                session=session,
                agency_ids=[agency.id for agency in allAgencies],
            )
            self.assertEqual(
                {a.name for a in agenciesById},
                {"Agency Alpha", "Beta Initiative", "2 System Initiative"},
            )

    @freeze_time("2023-09-17 12:00:01", tz_offset=0)
    def test_create_agency(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="test_auth0_user",
                email="test@email.com",
                auth0_client=self.test_auth0_client,
            )
            gamma_agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Gamma",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            delta_agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Delta",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            UserAccountInterface.add_or_update_user_agency_association(
                session=session, user=user, agencies=[delta_agency, gamma_agency]
            )
        session.add_all([user, gamma_agency, delta_agency])
        session.flush()
        agencies = AgencyInterface.get_agencies(session=session)
        self.assertEqual(
            {a.name for a in agencies},
            {"Agency Alpha", "Beta Initiative", "Agency Gamma", "Agency Delta"},
        )
        self.assertEqual(
            {
                a.created_at
                for a in agencies
                if a.name in ["Agency Gamma", "Agency Delta"]
            },
            {datetime.now(tz=timezone.utc)},
        )

        # Test to_json
        self.assertEqual(
            {a.to_json()["state"] for a in agencies},
            {"California", "Alaska"},
        )

        self.assertEqual(
            gamma_agency.to_json()["team"][0]["name"],
            user.name,
        )
        self.assertEqual(
            gamma_agency.to_json()["team"][0]["auth0_user_id"],
            user.auth0_user_id,
        )

        # Test non-unique agency
        try:
            gamma_agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Gamma",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
        except JusticeCountsServerError as e:
            self.assertEqual(e.code, "agency_already_exists")
            self.assertEqual(
                e.description,
                "Agency with name 'Agency Gamma' already exists with the state and the systems selected.",
            )

            gamma_agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Gamma",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ny",
                fips_county_code=None,
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            self.assertIsNotNone(gamma_agency)
            self.assertEqual(gamma_agency.name, "Agency Gamma")
            self.assertEqual(gamma_agency.state_code, "us_ny")
            self.assertEqual(
                gamma_agency.systems, [schema.System.LAW_ENFORCEMENT.value]
            )

    def test_get_child_agencies(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="test_auth0_user",
                email="test@email.com",
                auth0_client=self.test_auth0_client,
            )
            super_agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="Super Agency",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                is_superagency=True,
                agency_id=None,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            child_agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="Child Agency",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                super_agency_id=super_agency.id,
                agency_id=None,
                is_superagency=False,
                is_dashboard_enabled=False,
            )
            UserAccountInterface.add_or_update_user_agency_association(
                session=session, user=user, agencies=[super_agency, child_agency]
            )
        session.add_all([user, super_agency, child_agency])
        session.flush()
        # The super agency will have the child agency in its list of child agencies.
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=session, agency=super_agency
        )
        self.assertEqual(child_agencies, [child_agency])

        # The child agency will have no agencies in its list of child agencies.
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=session, agency=child_agency
        )
        self.assertEqual(child_agencies, [])

    def test_get_agency_dropdown_names(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            # Adding an agency with the state code in the name. The dropdown display
            # name should NOT append the state code if the state code is already in the
            # name.
            AgencyInterface.create_or_update_agency(
                session=session,
                name="GA County Court",
                systems=[schema.System.COURTS_AND_PRETRIAL],
                state_code="us_ga",
                fips_county_code="us_ga_fulton",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            agency_ids = [
                agency.id for agency in AgencyInterface.get_agencies(session=session)
            ]
            ids_to_dropdown_names = AgencyInterface.get_agency_dropdown_names(
                session=session, agency_ids=agency_ids
            )
            self.assertEqual(
                list(ids_to_dropdown_names.values()),
                ["Agency Alpha (CA)", "Beta Initiative (AK)", "GA County Court"],
            )

    def test_get_dashboard_homepage_json(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            # Adding an agency with the state code in the name. The dropdown display
            # name should NOT append the state code if the state code is already in the
            # name.
            agency = AgencyInterface.create_or_update_agency(
                session=session,
                name="GA County Court",
                systems=[schema.System.COURTS_AND_PRETRIAL],
                state_code="us_ga",
                fips_county_code="us_ga_fulton",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=True,
            )
            fips_code_to_geoid = get_fips_code_to_geoid()
            county_code_to_county_name = get_county_code_to_county_name()
            county_code_to_county_fips = get_county_code_to_county_fips()

            json = AgencyInterface.get_dashboard_homepage_json(
                fips_code_to_geoid=fips_code_to_geoid,
                county_code_to_county_name=county_code_to_county_name,
                county_code_to_county_fips=county_code_to_county_fips,
                agency=agency,
                metric_key_to_metric_interface={},
                metric_key_dim_id_to_available_members={},
            )

            self.assertEqual(
                json,
                {
                    "id": agency.id,
                    "name": "GA County Court",
                    "available_sectors": [],
                    "available_metric_keys": [],
                    "available_disaggregations": {},
                    "is_dashboard_enabled": True,
                    "is_demo": False,
                    "state_geoid": "0400000US13",
                    "county_geoid": None,
                    "state_name": "Georgia",
                    "county_name": "Fulton County",
                },
            )

            session.commit()
            json = AgencyInterface.get_dashboard_homepage_json(
                fips_code_to_geoid=fips_code_to_geoid,
                county_code_to_county_name=county_code_to_county_name,
                county_code_to_county_fips=county_code_to_county_fips,
                agency=agency,
                metric_key_to_metric_interface={
                    "COURTS_AND_PRETRIAL_FUNDING": MetricInterface(
                        key="COURTS_AND_PRETRIAL_FUNDING",
                        is_metric_enabled=True,
                        aggregated_dimensions=[
                            MetricAggregatedDimensionData(
                                dimension_to_enabled_status={
                                    member: True for member in FundingType
                                }
                            )
                        ],
                    ),
                    "COURTS_AND_PRETRIAL_TOTAL_STAFF": MetricInterface(
                        key="COURTS_AND_PRETRIAL_TOTAL_STAFF",
                        is_metric_enabled=True,
                        aggregated_dimensions=[
                            MetricAggregatedDimensionData(
                                dimension_to_enabled_status={
                                    member: len(member.dimension_name) % 2 == 0
                                    for member in StaffType
                                }
                            )
                        ],
                    ),
                    "COURTS_AND_PRETRIAL_PRETRIAL_RELEASES": MetricInterface(
                        key="COURTS_AND_PRETRIAL_PRETRIAL_RELEASES",
                        is_metric_enabled=False,
                    ),
                    "COURTS_AND_PRETRIAL_SENTENCES": MetricInterface(
                        key="COURTS_AND_PRETRIAL_SENTENCES",
                        is_metric_enabled=True,
                        aggregated_dimensions=[
                            MetricAggregatedDimensionData(
                                dimension_to_enabled_status={
                                    member: False for member in RaceAndEthnicity
                                }
                            ),
                            MetricAggregatedDimensionData(
                                dimension_to_enabled_status={
                                    member: False for member in BiologicalSex
                                }
                            ),
                            MetricAggregatedDimensionData(
                                dimension_to_enabled_status={
                                    member: False for member in SentenceType
                                }
                            ),
                        ],
                    ),
                },
                metric_key_dim_id_to_available_members={
                    "COURTS_AND_PRETRIAL_TOTAL_STAFF": {
                        "metric/courts/staff/type": {
                            "SECURITY",
                            "ADMINISTRATIVE",
                            "JUDGES",
                            "OTHER",
                            "VACANT",
                            "LEGAL",
                            "ADVOCATE",
                            "UNKNOWN",
                        }
                    },
                    "COURTS_AND_PRETRIAL_FUNDING": {
                        "metric/courts/funding/type": {
                            "UNKNOWN",
                            "STATE_APPROPRIATION",
                            "GRANTS",
                        }
                    },
                    "COURTS_AND_PRETRIAL_SENTENCES": {None: {None}},
                },
            )

            self.assertEqual(
                json,
                {
                    "id": agency.id,
                    "name": "GA County Court",
                    "available_sectors": ["COURTS_AND_PRETRIAL"],
                    "available_metric_keys": [
                        "COURTS_AND_PRETRIAL_FUNDING",
                        "COURTS_AND_PRETRIAL_TOTAL_STAFF",
                        "COURTS_AND_PRETRIAL_SENTENCES",
                    ],
                    "available_disaggregations": {
                        "COURTS_AND_PRETRIAL_FUNDING": {
                            "metric/courts/funding/type": [
                                "STATE_APPROPRIATION",
                                "GRANTS",
                                "UNKNOWN",
                            ]
                        },
                        "COURTS_AND_PRETRIAL_TOTAL_STAFF": {
                            "metric/courts/staff/type": [
                                "JUDGES",
                                "SECURITY",
                                "ADMINISTRATIVE",
                                "ADVOCATE",
                                "VACANT",
                            ],
                        },
                    },
                    "is_dashboard_enabled": True,
                    "is_demo": False,
                    "state_geoid": "0400000US13",
                    "county_geoid": None,
                    "state_name": "Georgia",
                    "county_name": "Fulton County",
                },
            )
