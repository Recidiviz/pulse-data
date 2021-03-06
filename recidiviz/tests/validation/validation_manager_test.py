# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for validation/validation_manager.py."""

from typing import List, Set
from unittest import TestCase

from flask import Flask
from mock import patch, call, MagicMock

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.namespaces import BigQueryViewNamespace
from recidiviz.tests.utils.matchers import UnorderedCollection
from recidiviz.utils.environment import GCPEnvironment
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.checks.sameness_check import (
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
)
from recidiviz.validation.configured_validations import (
    get_all_validations,
    get_validation_region_configs,
    get_validation_global_config,
)
from recidiviz.validation.validation_config import (
    ValidationRegionConfig,
    ValidationNumAllowedRowsOverride,
    ValidationMaxAllowedErrorOverride,
)
from recidiviz.validation.validation_manager import (
    validation_manager_blueprint,
    _fetch_validation_jobs_to_perform,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationCheckType,
)
from recidiviz.calculator.query.county import view_config as county_view_config
from recidiviz.calculator.query.justice_counts import (
    view_config as justice_counts_view_config,
)
from recidiviz.calculator.query.state import view_config as state_view_config
from recidiviz.case_triage.views import view_config as case_triage_view_config
from recidiviz.ingest.direct.views import view_config as direct_ingest_view_config
from recidiviz.ingest.views import view_config as ingest_metadata_view_config
from recidiviz.validation.views import view_config as validation_view_config


def get_test_validations() -> List[DataValidationJob]:
    return [
        DataValidationJob(
            region_code="US_UT",
            validation=ExistenceDataValidationCheck(
                view=BigQueryView(
                    dataset_id="my_dataset",
                    view_id="test_1",
                    description="test_1 description",
                    view_query_template="select * from literally_anything",
                )
            ),
        ),
        DataValidationJob(
            region_code="US_UT",
            validation=ExistenceDataValidationCheck(
                view=BigQueryView(
                    dataset_id="my_dataset",
                    view_id="test_2",
                    description="test_2 description",
                    view_query_template="select * from literally_anything",
                )
            ),
        ),
        DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                view=BigQueryView(
                    dataset_id="my_dataset",
                    view_id="test_1",
                    description="test_1 description",
                    view_query_template="select * from literally_anything",
                )
            ),
        ),
        DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                view=BigQueryView(
                    dataset_id="my_dataset",
                    view_id="test_2",
                    description="test_2 description",
                    view_query_template="select * from literally_anything",
                )
            ),
        ),
    ]


_API_RESPONSE_IF_NO_FAILURES = "Failed validations:\n"


class TestHandleRequest(TestCase):
    """Tests for the Validation Manager API endpoint."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        app = Flask(__name__)
        app.register_blueprint(validation_manager_blueprint)
        app.config["TESTING"] = True
        self.client = app.test_client()

        self._TEST_VALIDATIONS = get_test_validations()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()

    @patch("recidiviz.big_query.view_update_manager.rematerialize_views_for_namespace")
    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_no_failures(
        self,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_failures: MagicMock,
        mock_rematerialize_views: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        mock_run_job.return_value = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[0],
            was_successful=True,
            failure_description=None,
        )

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/validate", headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        self.assertEqual(4, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_rematerialize_views.assert_called()
        mock_emit_failures.assert_not_called()

    @patch("recidiviz.big_query.view_update_manager.rematerialize_views_for_namespace")
    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_with_job_failures_and_validation_failures(
        self,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_failures: MagicMock,
        mock_rematerialize_views: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS
        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            was_successful=False,
            failure_description="Oh no",
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            was_successful=False,
            failure_description="How awful",
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                was_successful=True,
                failure_description=None,
            ),
            first_failure,
            second_failure,
            ValueError("Job failed to run!"),
        ]
        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/validate", headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertNotEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        self.assertEqual(4, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_rematerialize_views.assert_called()
        mock_emit_failures.assert_called_with(
            UnorderedCollection([self._TEST_VALIDATIONS[3]]),
            UnorderedCollection([first_failure, second_failure]),
        )

    @patch("recidiviz.big_query.view_update_manager.rematerialize_views_for_namespace")
    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_some_failures(
        self,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_failures: MagicMock,
        mock_rematerialize_views: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = self._TEST_VALIDATIONS

        first_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[1],
            was_successful=False,
            failure_description="Oh no",
        )
        second_failure = DataValidationJobResult(
            validation_job=self._TEST_VALIDATIONS[2],
            was_successful=False,
            failure_description="How awful",
        )
        mock_run_job.side_effect = [
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[0],
                was_successful=True,
                failure_description=None,
            ),
            first_failure,
            second_failure,
            DataValidationJobResult(
                validation_job=self._TEST_VALIDATIONS[3],
                was_successful=True,
                failure_description=None,
            ),
        ]

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/validate", headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertNotEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        self.assertEqual(4, mock_run_job.call_count)
        for job in self._TEST_VALIDATIONS:
            mock_run_job.assert_any_call(job)

        mock_rematerialize_views.assert_called()
        mock_emit_failures.assert_called_with(
            [], UnorderedCollection([first_failure, second_failure])
        )

    @patch("recidiviz.big_query.view_update_manager.rematerialize_views_for_namespace")
    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_nothing_configured(
        self,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_failures: MagicMock,
        mock_rematerialize_views: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = []

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/validate", headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        mock_rematerialize_views.assert_called()
        mock_run_job.assert_not_called()
        mock_emit_failures.assert_not_called()

    @patch("recidiviz.big_query.view_update_manager.rematerialize_views_for_namespace")
    @patch("recidiviz.validation.validation_manager._emit_failures")
    @patch("recidiviz.validation.validation_manager._run_job")
    @patch("recidiviz.validation.validation_manager._fetch_validation_jobs_to_perform")
    def test_handle_request_happy_path_should_update_views(
        self,
        mock_fetch_validations: MagicMock,
        mock_run_job: MagicMock,
        mock_emit_failures: MagicMock,
        mock_rematerialize_views: MagicMock,
    ) -> None:
        mock_fetch_validations.return_value = []

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get("/validate", headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(_API_RESPONSE_IF_NO_FAILURES, response.get_data().decode())

        mock_run_job.assert_not_called()
        mock_emit_failures.assert_not_called()

        self.maxDiff = None
        expected_update_calls = [
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.COUNTY,
                candidate_view_builders=county_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.JUSTICE_COUNTS,
                candidate_view_builders=justice_counts_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.STATE,
                candidate_view_builders=state_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.VALIDATION,
                candidate_view_builders=validation_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.CASE_TRIAGE,
                candidate_view_builders=case_triage_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.INGEST_METADATA,
                candidate_view_builders=ingest_metadata_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.VALIDATION_METADATA,
                candidate_view_builders=validation_view_config.METADATA_VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
            call(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=BigQueryViewNamespace.DIRECT_INGEST,
                candidate_view_builders=direct_ingest_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
                dataset_overrides=None,
            ),
        ]
        self.assertCountEqual(
            mock_rematerialize_views.call_args_list, expected_update_calls
        )


class TestFetchValidations(TestCase):
    """Tests the _fetch_validation_jobs_to_perform function."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        return_value=GCPEnvironment.STAGING.value,
    )
    def test_cross_product_states_and_checks_staging(
        self, _mock_get_environment: MagicMock
    ) -> None:
        all_validations = get_all_validations()
        all_region_configs = get_validation_region_configs()
        global_config = get_validation_global_config()
        all_regions = all_region_configs.keys()

        num_exclusions = sum(
            [len(config.exclusions) for config in all_region_configs.values()]
        ) + len(global_config.disabled) * len(all_regions)

        expected_length = len(all_validations) * len(all_regions) - num_exclusions

        result = _fetch_validation_jobs_to_perform()
        self.assertEqual(expected_length, len(result))

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        return_value=GCPEnvironment.PRODUCTION.value,
    )
    def test_cross_product_states_and_checks_production(
        self, _mock_get_environment: MagicMock
    ) -> None:
        all_validations = get_all_validations()
        region_configs_to_validate = get_validation_region_configs()
        global_config = get_validation_global_config()
        state_codes_to_validate = region_configs_to_validate.keys()

        # When you promote a state to production, we will start running validations against that state - add it to this
        # list to confirm you've updated all relevant external data validation tables in production to include
        # validation data for the newly promoted region.
        self.assertCountEqual(
            state_codes_to_validate, ["US_ID", "US_MO", "US_ND", "US_PA"]
        )

        num_exclusions = sum(
            [len(config.exclusions) for config in region_configs_to_validate.values()]
        ) + len(global_config.disabled) * len(state_codes_to_validate)

        expected_length = (
            len(all_validations) * len(state_codes_to_validate) - num_exclusions
        )

        result = _fetch_validation_jobs_to_perform()
        self.assertEqual(expected_length, len(result))

    @patch("recidiviz.validation.validation_manager.get_validation_region_configs")
    @patch("recidiviz.validation.validation_manager.get_all_validations")
    def test_fetch_validation_jobs_to_perform_applies_configs(
        self,
        mock_get_all_validations_fn: MagicMock,
        mock_get_region_configs_fn: MagicMock,
    ) -> None:
        existence_view = BigQueryView(
            project_id="my_project",
            dataset_id="my_dataset",
            view_id="existence_view",
            description="existence_view description",
            view_query_template="SELECT NULL LIMIT 0",
        )
        sameness_view = BigQueryView(
            project_id="my_project",
            dataset_id="my_dataset",
            view_id="sameness_view",
            description="sameness_view description",
            view_query_template="SELECT NULL LIMIT 1",
        )
        mock_get_all_validations_fn.return_value = [
            ExistenceDataValidationCheck(view=existence_view),
            SamenessDataValidationCheck(
                view=sameness_view, comparison_columns=["col1", "col2"]
            ),
        ]
        mock_get_region_configs_fn.return_value = {
            "US_XX": ValidationRegionConfig(
                region_code="US_XX",
                exclusions={},
                num_allowed_rows_overrides={
                    existence_view.view_id: ValidationNumAllowedRowsOverride(
                        region_code="US_XX",
                        validation_name=existence_view.view_id,
                        num_allowed_rows_override=10,
                        override_reason="This is broken",
                    )
                },
                max_allowed_error_overrides={
                    sameness_view.view_id: ValidationMaxAllowedErrorOverride(
                        region_code="US_XX",
                        validation_name=sameness_view.view_id,
                        max_allowed_error_override=0.3,
                        override_reason="This is also broken",
                    )
                },
            ),
            "US_YY": ValidationRegionConfig(
                region_code="US_YY",
                exclusions={},
                num_allowed_rows_overrides={},
                max_allowed_error_overrides={},
            ),
        }
        result = _fetch_validation_jobs_to_perform()

        expected_jobs = [
            DataValidationJob(
                validation=ExistenceDataValidationCheck(
                    view=existence_view,
                    validation_name_suffix=None,
                    validation_type=ValidationCheckType.EXISTENCE,
                    num_allowed_rows=10,
                ),
                region_code="US_XX",
            ),
            DataValidationJob(
                validation=ExistenceDataValidationCheck(
                    view=existence_view,
                    validation_name_suffix=None,
                    validation_type=ValidationCheckType.EXISTENCE,
                    num_allowed_rows=0,
                ),  # No override
                region_code="US_YY",
            ),
            DataValidationJob(
                validation=SamenessDataValidationCheck(
                    view=sameness_view,
                    validation_name_suffix=None,
                    comparison_columns=["col1", "col2"],
                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                    max_allowed_error=0.3,
                    validation_type=ValidationCheckType.SAMENESS,
                ),
                region_code="US_XX",
            ),
            DataValidationJob(
                validation=SamenessDataValidationCheck(
                    view=sameness_view,
                    validation_name_suffix=None,
                    comparison_columns=["col1", "col2"],
                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                    max_allowed_error=0.0,
                    validation_type=ValidationCheckType.SAMENESS,
                ),
                region_code="US_YY",
            ),
        ]
        self.assertEqual(expected_jobs, result)

    def test_all_validations_no_overlapping_names(self) -> None:
        all_validations = get_all_validations()

        all_names: Set[str] = set()
        for validation in all_validations:
            self.assertNotIn(validation.validation_name, all_names)
            all_names.add(validation.validation_name)

    def test_configs_all_reference_real_validations(self) -> None:
        validation_names = {
            validation.validation_name for validation in get_all_validations()
        }
        region_configs_to_validate = get_validation_region_configs()
        global_config = get_validation_global_config()

        global_disabled_names = {
            validation.validation_name for validation in global_config.disabled.values()
        }

        global_names_not_in_validations_list = global_disabled_names.difference(
            validation_names
        )
        self.assertEqual(
            set(),
            global_names_not_in_validations_list,
            f"Found views referenced in global config that do not exist in validations list: "
            f"{global_names_not_in_validations_list}",
        )

        for region_code, region_config in region_configs_to_validate.items():
            region_validation_names = {
                exclusion.validation_name
                for exclusion in region_config.exclusions.values()
            }
            region_validation_names.update(
                {
                    override.validation_name
                    for override in region_config.max_allowed_error_overrides.values()
                }
            )
            region_validation_names.update(
                {
                    override.validation_name
                    for override in region_config.num_allowed_rows_overrides.values()
                }
            )
            region_names_not_in_validations_list = region_validation_names.difference(
                validation_names
            )
            self.assertEqual(
                set(),
                region_names_not_in_validations_list,
                f"Found views referenced in region [{region_code}] config that do not exist in validations"
                f" list: {global_names_not_in_validations_list}",
            )

    def test_all_views_referenced_by_validations_are_in_view_config(self) -> None:
        view_in_config = [
            view_builder.build()
            for view_builder in validation_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE
        ]
        views_in_validations = {v.view for v in get_all_validations()}
        validation_views_not_in_view_config = views_in_validations.difference(
            view_in_config
        )

        self.assertEqual(set(), validation_views_not_in_view_config)

    def test_validation_check_returns_correct_query(self) -> None:
        view_no_overrides = BigQueryView(
            dataset_id="my_dataset",
            view_id="test_2",
            description="test_2 description",
            view_query_template="select * from literally_anything",
        )
        view_with_overrides = BigQueryView(
            dataset_id="my_dataset",
            view_id="test_2",
            description="test_2 description",
            view_query_template="select * from literally_anything",
            dataset_overrides={"my_dataset": "my_dataset_override"},
        )
        existence_check = ExistenceDataValidationCheck(view=view_no_overrides)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.my_dataset.test_2` WHERE region_code = 'US_XX';",
            existence_check.query_str_for_region_code("US_XX"),
        )
        existence_check = ExistenceDataValidationCheck(view=view_with_overrides)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.my_dataset_override.test_2` WHERE region_code = 'US_XX';",
            existence_check.query_str_for_region_code("US_XX"),
        )
