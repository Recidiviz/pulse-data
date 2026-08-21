# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""This class implements tests for Pathways api routes."""
import csv
import io
import os
from datetime import date
from http import HTTPStatus
from typing import Callable, Dict, Optional
from unittest import mock
from unittest.case import TestCase
from unittest.mock import MagicMock, patch

import pytest
from fakeredis import FakeRedis
from flask import Flask
from flask.testing import FlaskClient
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.public_pathways.schema import (
    MetricMetadata,
    PublicPrisonPopulationOverTime,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.public_pathways.individual_level_bulk_export import bulk_export_gcs_path
from recidiviz.public_pathways.public_pathways_authorization import (
    on_successful_authorization,
)
from recidiviz.public_pathways.public_pathways_routes import (
    create_public_pathways_api_blueprint,
)
from recidiviz.tests.case_triage.shared_pathways.fixture_helpers import (
    load_metrics_fixture,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.public_pathways import fixtures
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


class PublicPathwaysBlueprintTestCase(TestCase):
    """Base class for Public Pathways flask tests"""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        self.mock_authorization_handler = MagicMock()

        self.fake_redis = FakeRedis()
        self.redis_patcher = mock.patch(
            "recidiviz.case_triage.shared_pathways.metric_cache.get_public_pathways_metric_redis",
            return_value=self.fake_redis,
        )
        self.redis_patcher.start()

        self.individual_level_export_redis_patcher = mock.patch(
            "recidiviz.public_pathways.individual_level_export.get_public_pathways_metric_redis",
            return_value=self.fake_redis,
        )
        self.individual_level_export_redis_patcher.start()

        self.auth_patcher = mock.patch(
            "recidiviz.case_triage.shared_pathways.shared_pathways_blueprint.build_authorization_handler",
            return_value=self.mock_authorization_handler,
        )

        self.auth_patcher.start()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        limiter = Limiter(key_func=get_remote_address, app=self.test_app)
        self.test_app.register_blueprint(
            create_public_pathways_api_blueprint(limiter),
            url_prefix="/public_pathways",
        )
        self.test_client = self.test_app.test_client()

    def tearDown(self) -> None:
        self.auth_patcher.stop()
        self.redis_patcher.stop()
        self.individual_level_export_redis_patcher.stop()

    @staticmethod
    def auth_side_effect(
        state_code: str,
        allowed_states: Optional[list[str]] = None,
    ) -> Callable:
        if allowed_states is None:
            allowed_states = []

        return lambda: on_successful_authorization(
            {
                f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                    "stateCode": f"{state_code.lower()}",
                    "allowedStates": allowed_states,
                },
            }
        )


@pytest.mark.uses_db
class TestPublicPathwaysMetrics(PublicPathwaysBlueprintTestCase):
    """Implements tests for the Public Pathways routes."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        super().setUp()

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="recidiviz", allowed_states=["US_NY"]
        )

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

        self.database_key = SQLAlchemyDatabaseKey(
            SchemaType.PUBLIC_PATHWAYS, db_name="us_ny"
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

        self.count_by_dimension_metric_path = (
            "/public_pathways/US_NY/PrisonPopulationByDimensionCount"
        )

        self.over_time_metric_path = "/public_pathways/US_NY/PrisonPopulationOverTime"

        self.person_level_metric_path = (
            "/public_pathways/US_NY/PrisonPopulationPersonLevel"
        )

        self.individual_level_export_path = (
            "/public_pathways/US_NY/PrisonPopulationIndividualLevel"
        )

        with SessionFactory.using_database(self.database_key) as session:
            # PrisonPopulationByDimension and PrisonPopulationOverTime share the
            # same table, so we only need to load the data once.
            for metric in load_metrics_fixture(
                PublicPrisonPopulationOverTime, fixtures
            ):
                session.add(PublicPrisonPopulationOverTime(**metric))
            for metadata in load_metrics_fixture(MetricMetadata, fixtures):
                session.add(MetricMetadata(**metadata))

    def tearDown(self) -> None:
        super().tearDown()

        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.environ.pop("AUTH0_CLAIM_NAMESPACE", None)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_metrics_unauthorized_recidiviz_users(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="recidiviz", allowed_states=[]
        )
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"group": Dimension.AGE_GROUP.value},
        )
        self.assertEqual(
            HTTPStatus.UNAUTHORIZED, response.status_code, response.get_json()
        )

    def test_metrics_invalid_params(self) -> None:
        # Requesting fake metric
        response = self.test_client.get(
            "/public_pathways/US_NY/FakeMetric",
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": "FakeMetric is not enabled for US_NY"},
        )

        # Requesting real metric without group
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(
            HTTPStatus.BAD_REQUEST, response.status_code, response.get_json()
        )
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": {"group": ["Missing data for required field."]}},
        )

        # Requesting real metric with fake grouping column
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"group": "fake"},
        )
        self.assertEqual(
            HTTPStatus.BAD_REQUEST, response.status_code, response.get_json()
        )
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {
                "description": {
                    "group": [
                        "Must be one of: admission_reason, age_group, "
                        "facility, gender, sex, judicial_district, legal_status, "
                        "length_of_stay, prior_length_of_incarceration, "
                        "race, ethnicity, religion, marital_status, months_at_facility, "
                        "supervising_officer, supervision_district, "
                        "district, supervision_level, "
                        "supervision_start_date, supervision_type, "
                        "time_period, year_month, start_date, end_date, "
                        "avg_daily_population, "
                        "avg_population_limited_supervision_level, "
                        "months_since_treatment, sentence_length_min, sentence_length_max, "
                        "charge_county_code, offense_type, charge_description, date_in_population."
                    ]
                }
            },
        )

    def test_metrics_base(self) -> None:
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"group": Dimension.AGE_GROUP.value},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            response.get_json(),
            {
                "data": [
                    {"count": 1, "ageGroup": "25-29"},
                    {"count": 1, "ageGroup": "60+"},
                ],
                "metadata": {
                    "dynamicFilterOptions": '{"gender_id_name_map": '
                    '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}], '
                    '"date_in_population_id_name_map": '
                    '[{"value": "2022-01-01", "label": "January 1, 2022"}, '
                    '{"value": "2021-12-01", "label": "December 1, 2021"}, '
                    '{"value": "2021-11-01", "label": "November 1, 2021"}]}',
                    "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, '
                    '{"value": "2", "label": "Facility 2"}]',
                    "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}]',
                    "lastUpdated": "2022-08-03",
                },
            },
        )

        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={
                "group": Dimension.RACE.value,
                f"filters[{Dimension.RACE.value}]": "BLACK",
            },
        )
        self.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        self.assertEqual(
            {
                "data": [
                    {"count": 1, "race": "BLACK"},
                ],
                "metadata": {
                    "dynamicFilterOptions": '{"gender_id_name_map": '
                    '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}], '
                    '"date_in_population_id_name_map": '
                    '[{"value": "2022-01-01", "label": "January 1, 2022"}, '
                    '{"value": "2021-12-01", "label": "December 1, 2021"}, '
                    '{"value": "2021-11-01", "label": "November 1, 2021"}]}',
                    "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, '
                    '{"value": "2", "label": "Facility 2"}]',
                    "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}]',
                    "lastUpdated": "2022-08-03",
                },
            },
            response.get_json(),
        )

    def test_person_level_metrics(self) -> None:
        response = self.test_client.get(
            self.person_level_metric_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": "PrisonPopulationPersonLevel is not enabled for US_NY"},
        )

    def test_individual_level_export(self) -> None:
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_data())
        self.assertEqual("text/csv; charset=utf-8", response.content_type)
        self.assertEqual(
            'attachment; filename="us_ny_individual_level_data_2022-08-03.csv"',
            response.headers["Content-Disposition"],
        )

        rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))

        self.assertNotIn("gender", rows[0])
        self.assertNotIn("person_id", rows[0])

        # Only the most recent date_in_population (2022-01-01) is included, even
        # though the fixture has rows for 2021-11-01 and 2021-12-01 too.
        self.assertEqual(
            [
                {
                    "state_code": "US_XX",
                    "date_in_population": "2022-01-01",
                    "age_group": "25-29",
                    "facility": "F1",
                    "sex": "MALE",
                    "race": "WHITE",
                    "ethnicity": "NOT_HISPANIC",
                    "months_at_facility": "3_5_MONTHS",
                    "sentence_length_min_in_months": "48-71 MONTHS",
                    "sentence_length_max_in_months": "72-95 MONTHS",
                    "charge_county_code": "COUNTY_1",
                    "offense_type": "DRUG OFFENSES",
                    "charge_description": "ALL OTHER FELONIES",
                    "admission_reason": "NEW_COURT_COMMITMENT",
                },
                {
                    "state_code": "US_XX",
                    "date_in_population": "2022-01-01",
                    "age_group": "60+",
                    "facility": "F1",
                    "sex": "MALE",
                    "race": "BLACK",
                    "ethnicity": "NOT_HISPANIC",
                    "months_at_facility": "3_5_MONTHS",
                    "sentence_length_min_in_months": "48-71 MONTHS",
                    "sentence_length_max_in_months": "72-95 MONTHS",
                    "charge_county_code": "COUNTY_1",
                    "offense_type": "DRUG OFFENSES",
                    "charge_description": "ALL OTHER FELONIES",
                    "admission_reason": "NEW_COURT_COMMITMENT",
                },
            ],
            rows,
        )

    def test_individual_level_export_uses_cache(self) -> None:
        first_response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.OK, first_response.status_code)

        # The key ends in a hash of the label maps, so match the stable prefix
        # rather than pinning the digest.
        cached_keys = self.fake_redis.keys(
            "US_NY individual_level_export 2022-08-03 local *"
        )
        self.assertEqual(1, len(cached_keys))

        # Deleting the underlying rows proves the second request is served from
        # the cache rather than re-querying: a fresh query would now return no
        # rows, but MetricMetadata (and therefore the cache key) is untouched.
        with SessionFactory.using_database(self.database_key) as session:
            session.query(PublicPrisonPopulationOverTime).delete()

        second_response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.OK, second_response.status_code)
        self.assertEqual(first_response.get_data(), second_response.get_data())

    def test_individual_level_export_specific_month(self) -> None:
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"year": "2021", "month": "12"},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_data())
        self.assertEqual(
            'attachment; filename="us_ny_individual_level_data_2021-12-01.csv"',
            response.headers["Content-Disposition"],
        )

        rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
        self.assertEqual(3, len(rows))
        self.assertTrue(all(row["date_in_population"] == "2021-12-01" for row in rows))

    def test_individual_level_export_month_requires_both_params(self) -> None:
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"month": "12"},
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_individual_level_export_month_invalid_format(self) -> None:
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"year": "not-a-year", "month": "12"},
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_individual_level_export_month_no_snapshot_in_window(self) -> None:
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={"year": "2019", "month": "1"},
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_individual_level_export_invalid_state(self) -> None:
        response = self.test_client.get(
            "/public_pathways/NOTASTATE/PrisonPopulationIndividualLevel",
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_individual_level_export_state_partner_user_authorized(self) -> None:
        # Regression test: a real (non-Recidiviz) state partner user must be able to
        # reach this route, since it has no <metric_name> path segment for
        # on_successful_authorization to key off of like the metrics routes do.
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_NY"
        )
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_data())

    def test_individual_level_export_wrong_state_user_unauthorized(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_ID"
        )
        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(
            HTTPStatus.UNAUTHORIZED, response.status_code, response.get_json()
        )

    def test_individual_level_export_rate_limit(self) -> None:
        for _ in range(10):
            response = self.test_client.get(
                self.individual_level_export_path,
                headers={"Origin": "http://localhost:3050"},
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)

        response = self.test_client.get(
            self.individual_level_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.TOO_MANY_REQUESTS, response.status_code)

    @patch(
        "recidiviz.case_triage.shared_pathways.query_builders.over_time_metric_query_builder.func.current_date",
        return_value=date(2022, 3, 3),
    )
    def test_over_time_metrics(self, _mock_current_date: MagicMock) -> None:
        response = self.test_client.get(
            self.over_time_metric_path,
            headers={"Origin": "http://localhost:3050"},
            query_string={
                f"filters[{Dimension.TIME_PERIOD.value}]": "months_0_6",
            },
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            response.get_json(),
            {
                "data": [
                    {"avg90day": 1, "count": 2, "month": 11, "year": 2021},
                    {"avg90day": 2, "count": 3, "month": 12, "year": 2021},
                    {"avg90day": 2, "count": 2, "month": 1, "year": 2022},
                ],
                "metadata": {
                    "dynamicFilterOptions": '{"gender_id_name_map": '
                    '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}], '
                    '"date_in_population_id_name_map": '
                    '[{"value": "2022-01-01", "label": "January 1, 2022"}, '
                    '{"value": "2021-12-01", "label": "December 1, 2021"}, '
                    '{"value": "2021-11-01", "label": "November 1, 2021"}]}',
                    "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, '
                    '{"value": "2", "label": "Facility 2"}]',
                    "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}]',
                    "lastUpdated": "2022-08-03",
                },
            },
        )

        response = self.test_client.get(
            self.over_time_metric_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            response.get_json(),
            {
                "data": [
                    {"avg90day": 1, "count": 2, "month": 11, "year": 2021},
                    {"avg90day": 2, "count": 3, "month": 12, "year": 2021},
                    {"avg90day": 2, "count": 2, "month": 1, "year": 2022},
                ],
                "metadata": {
                    "dynamicFilterOptions": '{"gender_id_name_map": '
                    '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}], '
                    '"date_in_population_id_name_map": '
                    '[{"value": "2022-01-01", "label": "January 1, 2022"}, '
                    '{"value": "2021-12-01", "label": "December 1, 2021"}, '
                    '{"value": "2021-11-01", "label": "November 1, 2021"}]}',
                    "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, '
                    '{"value": "2", "label": "Facility 2"}]',
                    "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}]',
                    "lastUpdated": "2022-08-03",
                },
            },
        )

    def test_multiple_filters(self) -> None:
        # Filters for the same attribute are combined using boolean OR
        # Filters across attributes are combined using AND
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3050"},
            query_string=f"group={Dimension.RACE.value}"
            f"&filters[{Dimension.RACE.value}]=BLACK"
            f"&filters[{Dimension.RACE.value}]=WHITE"
            f"&filters[{Dimension.GENDER.value}]=MALE",
        )

        self.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        self.assertEqual(
            {
                "data": [
                    {"count": 1, "race": "BLACK"},
                    {"count": 1, "race": "WHITE"},
                ],
                "metadata": {
                    "dynamicFilterOptions": '{"gender_id_name_map": '
                    '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}], '
                    '"date_in_population_id_name_map": '
                    '[{"value": "2022-01-01", "label": "January 1, 2022"}, '
                    '{"value": "2021-12-01", "label": "December 1, 2021"}, '
                    '{"value": "2021-11-01", "label": "November 1, 2021"}]}',
                    "facilityIdNameMap": '[{"value": "1", "label": "Facility 1"}, '
                    '{"value": "2", "label": "Facility 2"}]',
                    "genderIdNameMap": '[{"value": "MALE", "label": "Male"}, '
                    '{"value": "FEMALE", "label": "Female"}, '
                    '{"value": "NON_BINARY", "label": "Non-binary"}]',
                    "lastUpdated": "2022-08-03",
                },
            },
            response.get_json(),
        )


@mock.patch(
    "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
)
class TestPublicPathwaysIndividualLevelExportBulk(PublicPathwaysBlueprintTestCase):
    """Implements tests for the bulk individual-level export route."""

    def setUp(self) -> None:
        super().setUp()

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_NY"
        )
        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

        self.bulk_export_path = (
            "/public_pathways/US_NY/PrisonPopulationIndividualLevelBulk"
        )

        self.fs = FakeGCSFileSystem()
        self.fs_patcher = mock.patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.fs_patcher.stop()
        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.environ.pop("AUTH0_CLAIM_NAMESPACE", None)

    def test_bulk_export_not_yet_precomputed(self) -> None:
        response = self.test_client.get(
            self.bulk_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_bulk_export_returns_precomputed_zip(self) -> None:
        gcs_path = bulk_export_gcs_path(state_code=StateCode.US_NY)
        self.fs.upload_from_string(gcs_path, "fake zip contents", "application/zip")
        self.fs.update_metadata(gcs_path, {"last_updated": "2022-08-03"})

        response = self.test_client.get(
            self.bulk_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_data())
        self.assertEqual("application/zip", response.content_type)
        self.assertEqual(
            'attachment; filename="us_ny_individual_level_data_last_5_years_2022-08-03.zip"',
            response.headers["Content-Disposition"],
        )
        self.assertEqual(b"fake zip contents", response.get_data())

    def test_bulk_export_wrong_state_user_unauthorized(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_ID"
        )
        response = self.test_client.get(
            self.bulk_export_path,
            headers={"Origin": "http://localhost:3050"},
        )
        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)


def make_cors_test(
    request_origin: str,
    request_method: str = "OPTIONS",
    expected_headers: Optional[Dict[str, str]] = None,
    expected_status: int = 200,
) -> Callable:
    def inner(self: PublicPathwaysBlueprintTestCase) -> None:
        response = self.test_client.open(
            method=request_method,
            path="/public_pathways/US_NY/PrisonPopulationByDimensionCount",
            headers={"Origin": request_origin},
        )

        self.assertEqual(expected_status, response.status_code, response.get_json())

        for header, value in (expected_headers or {}).items():
            self.assertEqual(value, response.headers[header])

    return inner


class TestPathwaysCORS(PublicPathwaysBlueprintTestCase):
    """Tests various CORS scenarios"""

    test_localhost_is_allowed = make_cors_test(
        request_origin="http://localhost:3050",
        expected_headers={
            "Access-Control-Allow-Origin": "http://localhost:3050",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, baggage",
            "Access-Control-Expose-Headers": "Content-Disposition",
            "Access-Control-Max-Age": "7200",
            "Vary": "Origin",
        },
    )

    test_staging_is_allowed = make_cors_test(
        request_origin="https://pathways-staging.recidiviz.org",
        expected_headers={
            "Access-Control-Allow-Origin": "https://pathways-staging.recidiviz.org",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, baggage",
            "Vary": "Origin",
        },
    )

    test_prod_is_allowed = make_cors_test(
        request_origin="https://pathways.recidiviz.org",
        expected_headers={
            "Access-Control-Allow-Origin": "https://pathways.recidiviz.org",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, baggage",
            "Vary": "Origin",
        },
    )

    test_cors_headers_sent_on_all_responses = make_cors_test(
        request_origin="http://localhost:3050",
        request_method="GET",
        expected_headers={
            "Access-Control-Allow-Origin": "http://localhost:3050",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, baggage",
            "Vary": "Origin",
        },
        expected_status=HTTPStatus.BAD_REQUEST,
    )

    test_spoof_preview_is_disallowed = make_cors_test(
        request_origin="https://recidiviz-dashboard-stag-e1108--officer-7tjx0jmi.fake.web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_spoof_preview_2_is_disallowed = make_cors_test(
        request_origin="https://preview.hacked.com/web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_wrong_localhost_port_is_disallowed = make_cors_test(
        request_origin="http://localhost:9999",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_firebase_preview_v1_is_allowed = make_cors_test(
        request_origin="https://public-pathways-staging--john-doe-foo-12345-v1-oi8f34kw.web.app",
        expected_headers={
            "Access-Control-Allow-Origin": "https://public-pathways-staging--john-doe-foo-12345-v1-oi8f34kw.web.app",
            "Vary": "Origin",
        },
    )

    test_firebase_preview_v2_is_allowed = make_cors_test(
        request_origin="https://public-pathways-staging--john-doe-foo-12345-v2-9zum45bs.web.app",
        expected_headers={
            "Access-Control-Allow-Origin": "https://public-pathways-staging--john-doe-foo-12345-v2-9zum45bs.web.app",
            "Vary": "Origin",
        },
    )

    test_firebase_preview_test_branch_is_allowed = make_cors_test(
        request_origin="https://public-pathways-staging--john-doe-foo-12345-v1-bg5evm16.web.app",
        expected_headers={
            "Access-Control-Allow-Origin": "https://public-pathways-staging--john-doe-foo-12345-v1-bg5evm16.web.app",
            "Vary": "Origin",
        },
    )

    test_firebase_preview_http_is_disallowed = make_cors_test(
        request_origin="http://public-pathways-staging--john-doe-foo-12345-v1-oi8f34kw.web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_firebase_preview_uppercase_hash_is_disallowed = make_cors_test(
        request_origin="https://public-pathways-staging--john-doe-foo-12345-v1-OI8F34KW.web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_firebase_preview_short_hash_is_disallowed = make_cors_test(
        request_origin="https://public-pathways-staging--john-doe-foo-12345-v1-oi8f34k.web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_firebase_preview_domain_injection_is_disallowed = make_cors_test(
        request_origin="https://public-pathways-staging--john-doe-foo-12345-v1-oi8f34kw.web.app.evil.com",
        expected_status=HTTPStatus.FORBIDDEN,
    )


class TestAuthorizationIntegration(PublicPathwaysBlueprintTestCase):
    """Tests that routes require authorization"""

    def test_all_non_options_request_require_authorization(self) -> None:
        self.test_client.get(
            "/public_pathways/US_NY/LibertyToPrisonTransitionsCount",
            query_string={"group": Dimension.RACE.value},
        )

        self.mock_authorization_handler.assert_called()

    def test_options_routes(self) -> None:
        self.test_client.options(
            "/public_pathways/US_NY/LibertyToPrisonTransitionsCount"
        )

        self.mock_authorization_handler.assert_not_called()

    @patch(
        "recidiviz.case_triage.shared_pathways.shared_pathways_blueprint.in_gcp_production",
        return_value=True,
    )
    def test_production_skips_authorization(
        self, _mock_in_production: MagicMock
    ) -> None:
        # Public Pathways is public in production, so the auth wall is dropped.
        self.test_client.get(
            "/public_pathways/US_NY/LibertyToPrisonTransitionsCount",
            query_string={"group": Dimension.RACE.value},
        )

        self.mock_authorization_handler.assert_not_called()

    @patch(
        "recidiviz.case_triage.shared_pathways.shared_pathways_blueprint.in_gcp_production",
        return_value=False,
    )
    def test_staging_requires_authorization(
        self, _mock_in_production: MagicMock
    ) -> None:
        # Outside of production (e.g. staging), authentication is still enforced.
        self.test_client.get(
            "/public_pathways/US_NY/LibertyToPrisonTransitionsCount",
            query_string={"group": Dimension.RACE.value},
        )

        self.mock_authorization_handler.assert_called()
