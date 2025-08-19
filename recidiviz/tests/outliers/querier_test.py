# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""This class implements tests for the OutliersQuerier class"""
from datetime import date, datetime
from unittest.mock import MagicMock, call, patch

import freezegun
import pytest
from dateutil.relativedelta import relativedelta
from sqlalchemy.exc import IntegrityError, NoResultFound

from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
    TIMELY_CONTACT,
    TIMELY_RISK_ASSESSMENT,
    VIOLATIONS,
)
from recidiviz.outliers.outliers_configs import TIMELY_CONTACT_DUE_DATE_BASED
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    ActionStrategyType,
    CaseloadCategory,
    ConfigurationStatus,
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersMetricConfig,
    UserInfo,
    VitalsMetric,
)
from recidiviz.persistence.database.schema.insights.schema import (
    ActionStrategySurfacedEvents,
    Configuration,
    MetricBenchmark,
    SupervisionClientEvent,
    SupervisionClients,
    SupervisionDistrictManager,
    SupervisionOfficer,
    SupervisionOfficerMetric,
    SupervisionOfficerOutlierStatus,
    SupervisionOfficerSupervisor,
    UserMetadata,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.insights.insights_db_test_case import InsightsDbTestCase
from recidiviz.tests.insights.utils import load_model_from_json_fixture

TEST_END_DATE = date(year=2023, month=5, day=1)
TEST_PREV_END_DATE = date(year=2023, month=4, day=1)

TEST_FEATURE_VARIANT = "testFV"
TEST_FEATURE_VARIANT_NOT_ASSIGNED = "dontGiveThisToUsers"


def build_test_metric_1(state_code: StateCode) -> OutliersMetricConfig:
    return OutliersMetricConfig.build_from_metric(
        state_code=state_code,
        metric=INCARCERATION_STARTS_AND_INFERRED,
        title_display_name="Incarceration Rate (CPVs & TPVs)",
        body_display_name="incarceration rate",
        event_name="incarcerations",
        event_name_singular="incarceration",
        event_name_past_tense="were incarcerated",
        description_markdown="""Incarceration rate description

<br />
Incarceration rate denominator description""",
        list_table_text="""Clients will appear on this list multiple times if they have been incarcerated more than once under this officer in the time period.""",
    )


def build_test_metric_2(state_code: StateCode) -> OutliersMetricConfig:
    return OutliersMetricConfig.build_from_metric(
        state_code=state_code,
        metric=TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
        title_display_name="Limited Supervision Unit Transfer Rate",
        body_display_name="Limited Supervision Unit transfer rate(s)",
        event_name="LSU transfers",
        event_name_singular="LSU transfer",
        event_name_past_tense="were transferred to LSU",
        inverse_feature_variant=TEST_FEATURE_VARIANT,
    )


def build_test_metric_3(state_code: StateCode) -> OutliersMetricConfig:
    return OutliersMetricConfig.build_from_metric(
        state_code=state_code,
        metric=ABSCONSIONS_BENCH_WARRANTS,
        title_display_name="Absconsion Rate",
        body_display_name="absconsion rate",
        event_name="absconsions",
        event_name_singular="absconsion",
        event_name_past_tense="absconded",
        is_absconsion_metric=True,
        list_table_text="""Clients will appear on this list multiple times if they have had more than one absconsion under this officer in the time period.""",
        rate_denominator="test_denominator",
        feature_variant=TEST_FEATURE_VARIANT,
    )


def build_test_metric_4(state_code: StateCode) -> OutliersMetricConfig:
    return OutliersMetricConfig.build_from_metric(
        state_code=state_code,
        metric=INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
        title_display_name="Technical Incarceration Rate",
        body_display_name="technical incarceration rate",
        event_name="technical incarcerations",
        event_name_singular="technical incarceration",
        event_name_past_tense="were incarcerated for a technical violation",
        feature_variant=TEST_FEATURE_VARIANT_NOT_ASSIGNED,
    )


TEST_CLIENT_EVENT_1 = OutliersClientEventConfig.build(
    event=VIOLATIONS, display_name="violations"
)


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class TestOutliersQuerier(InsightsDbTestCase):
    """Implements tests for the OutliersQuerier."""

    def setUp(self) -> None:
        super().setUp()

        with SessionFactory.using_database(self.insights_database_key) as session:
            # Restart the sequence in tests as per https://stackoverflow.com/questions/46841912/sqlalchemy-revert-auto-increment-during-testing-pytest
            session.execute("""ALTER SEQUENCE configurations_id_seq RESTART WITH 1;""")
            for config in load_model_from_json_fixture(Configuration):
                session.add(Configuration(**config))
            for metadata in load_model_from_json_fixture(UserMetadata):
                session.add(UserMetadata(**metadata))
            for officer in load_model_from_json_fixture(SupervisionOfficer):
                session.add(SupervisionOfficer(**officer))
            for status in load_model_from_json_fixture(SupervisionOfficerOutlierStatus):
                session.add(SupervisionOfficerOutlierStatus(**status))
            for supervisor in load_model_from_json_fixture(
                SupervisionOfficerSupervisor
            ):
                session.add(SupervisionOfficerSupervisor(**supervisor))
            for manager in load_model_from_json_fixture(SupervisionDistrictManager):
                session.add(SupervisionDistrictManager(**manager))
            for benchmark in load_model_from_json_fixture(MetricBenchmark):
                session.add(MetricBenchmark(**benchmark))
            for event in load_model_from_json_fixture(SupervisionClientEvent):
                session.add(SupervisionClientEvent(**event))
            for client in load_model_from_json_fixture(SupervisionClients):
                session.add(SupervisionClients(**client))
            for officer_metric in load_model_from_json_fixture(
                SupervisionOfficerMetric
            ):
                session.add(SupervisionOfficerMetric(**officer_metric))
            for as_event in load_model_from_json_fixture(ActionStrategySurfacedEvents):
                session.add(ActionStrategySurfacedEvents(**as_event))

        self.test_user_context = UserContext(
            email_address="tester@example.com",
            state_code_str="US_XX",
            user_external_id="12345",
            pseudonymized_id="hash-12345",
            can_access_all_supervisors=True,
            can_access_supervision_workflows=True,
            feature_variants=["supervisorHomepageWorkflows", TEST_FEATURE_VARIANT],
        )

        self.backend_config_patcher = patch(
            "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config",
            return_value=OutliersBackendConfig(
                metrics=[
                    build_test_metric_1(StateCode.US_XX),
                    build_test_metric_2(StateCode.US_XX),
                    build_test_metric_3(StateCode.US_XX),
                    build_test_metric_4(StateCode.US_XX),
                ],
            ),
        )
        self.backend_config_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.backend_config_patcher.stop()

    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_officer_level_report_data_by_supervisor(
        self, mock_config: MagicMock
    ) -> None:
        state_code = StateCode.US_XX
        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code), build_test_metric_2(state_code)],
        )

        actual = OutliersQuerier(
            state_code,
            [],  # this method is only used for emails, so more realistic to check the case with 0 FVs
        ).get_officer_level_report_data_for_all_officer_supervisors(
            end_date=TEST_END_DATE
        )

        self.snapshot.assert_match(actual, name="test_get_officer_level_report_data_by_supervisor")  # type: ignore[attr-defined]

        actual_json = {
            supervisor_id: supervisor_data.to_dict()
            for supervisor_id, supervisor_data in actual.items()
        }
        self.snapshot.assert_match(actual_json, name="test_get_officer_level_report_data_by_supervisor_json")  # type: ignore[attr-defined]

    def test_get_supervision_officer_supervisor_entities(
        self,
    ) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_supervisor_entities()

        self.snapshot.assert_match(actual, name="test_get_supervision_officer_supervisor_entities")  # type: ignore[attr-defined]

    def test_get_officers_for_supervisor(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_officers_for_supervisor(
            supervisor_external_id="102",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=5,
        )
        self.snapshot.assert_match(actual, name="test_get_officers_for_supervisor")  # type: ignore[attr-defined]

    def test_get_officers_for_supervisor_non_all_category(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_officers_for_supervisor(
            supervisor_external_id="102",
            category_type_to_compare=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
            include_workflows_info=True,
            num_lookback_periods=5,
        )
        self.snapshot.assert_match(actual, name="test_get_officers_for_supervisor_non_all_category")  # type: ignore[attr-defined]

    def test_get_officers_for_supervisor_without_workflows_info(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_officers_for_supervisor(
            supervisor_external_id="102",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=False,
            num_lookback_periods=5,
        )
        self.snapshot.assert_match(actual, name="test_get_officers_for_supervisor_without_workflows_info")  # type: ignore[attr-defined]

    def test_get_officer_outcomes_for_supervisor(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_officer_outcomes_for_supervisor(
            supervisor_external_id="102",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=5,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_for_supervisor")  # type: ignore[attr-defined]

    def test_get_officer_outcomes_for_supervisor_non_all_category(
        self,
    ) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_officer_outcomes_for_supervisor(
            supervisor_external_id="102",
            category_type_to_compare=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
            num_lookback_periods=5,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_for_supervisor_non_all_category")  # type: ignore[attr-defined]

    def test_get_supervisor_from_pseudonymized_id_no_match(self) -> None:
        # If matching supervisor doesn't exist, return None
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id="invalidhash",
        )
        self.assertIsNone(actual)

    def test_get_supervisor_from_pseudonymized_id_found_match(self) -> None:
        # Return matching supervisor
        with SessionFactory.using_database(self.insights_database_key) as session:
            expected = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "101")
                .first()
            )

            actual = OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            ).get_supervisor_entity_from_pseudonymized_id(
                supervisor_pseudonymized_id="hash1",
            )

            self.assertEqual(expected.external_id, actual.external_id)  # type: ignore[union-attr]

    def test_get_benchmarks(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_benchmarks(InsightsCaseloadCategoryType.ALL, 4)
        self.snapshot.assert_match(actual, name="test_get_benchmarks")  # type: ignore[attr-defined]

    def test_get_benchmarks_non_all_category(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_benchmarks(InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY, 4)
        self.snapshot.assert_match(actual, name="test_get_benchmarks_non_all_category")  # type: ignore[attr-defined]

    def test_get_events_by_officer(self) -> None:
        # Return matching event
        with SessionFactory.using_database(self.insights_database_key) as session:
            metric_id = build_test_metric_3(StateCode.US_XX).name
            expected = (
                session.query(SupervisionClientEvent)
                .filter(SupervisionClientEvent.event_date == "2023-04-01")
                .first()
            )

            actual = OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            ).get_events_by_officer("officerhash3", [metric_id])
            self.assertEqual(len(actual), 1)
            self.assertEqual(expected.event_date, actual[0].event_date)
            self.assertEqual(expected.client_id, actual[0].client_id)
            self.assertEqual(metric_id, actual[0].metric_id)

    def test_get_events_by_officer_none_found(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_events_by_officer("officerhash1", ["absconsions_bench_warrants"])
        self.assertEqual(actual, [])

    def test_get_supervision_officer_entity_found_match(self) -> None:
        # Return matching supervision officer entity
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )
        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entity_found_match")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entity_found_match_without_workflows_info(
        self,
    ) -> None:
        # Return matching supervision officer entity
        entity = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=False,
            num_lookback_periods=0,
        )
        self.assertIsNotNone(entity)
        if entity:
            self.assertIsNone(entity.zero_grant_opportunities)

    def test_get_supervision_officer_entity_found_match_for_officer_with_low_activity(
        self,
    ) -> None:
        # Return matching supervision officer entity
        entity = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash9",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )
        self.assertIsNotNone(entity)
        if entity:
            self.assertEqual(entity.zero_grant_opportunities, [])

    def test_get_supervision_officer_entity_found_match_for_officer_with_null_avg_population(
        self,
    ) -> None:
        # Return matching supervision officer entity
        entity = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash5",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )
        self.assertIsNotNone(entity)
        if entity:
            self.assertEqual(entity.avg_daily_population, None)

    def test_get_supervision_officer_entity_found_match_with_highlights(self) -> None:
        # Return matching supervision officer entity where officer should be highlighted for a metric in the latest period
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )
        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entity_found_match_with_highlights")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entity_found_match_not_top_x_pct(self) -> None:
        # Return matching supervision officer entity where officer has highlight information, but should not be highlighted
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash9",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )
        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entity_found_match_not_top_x_pct")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entity_found_match_uses_most_recent_avg_caseload(
        self,
    ) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )
        self.assertEqual(actual.avg_daily_population, 54.321)  # type: ignore[union-attr]

    def test_get_supervision_officer_entity_found_match_multiperiod_uses_most_recent_avg_caseload(
        self,
    ) -> None:
        # Return matching supervision officer entity with multiple lookback periods
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=4,
        )
        self.assertEqual(actual.avg_daily_population, 54.321)  # type: ignore[union-attr]

    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_supervision_officer_entity_highlight_in_prev_period_only(
        self, mock_config: MagicMock
    ) -> None:
        state_code = StateCode.US_XX
        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_2(state_code)],
        )

        # Return matching supervision officer entity where officer is highlighted in a previous period but not the latest
        actual = OutliersQuerier(
            state_code, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash7",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=1,
        )
        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entity_highlight_in_prev_period_only")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entity_changing_caseload_categories(self) -> None:
        # Return matching supervision officer entity where officer has changing caseload categories over time
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="officerhash1",
            category_type_to_compare=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
            include_workflows_info=True,
            num_lookback_periods=1,
        )
        self.snapshot.assert_match(actual, name="test_get_supervision_officer_entity_changing_caseload_categories")  # type: ignore[attr-defined]

    def test_get_supervision_officer_entity_included_in_outcomes(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            # officer fixture where include_in_outcomes=True
            pseudonymized_officer_id="officerhash4",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )

        self.assertIsNotNone(actual)
        self.assertTrue(actual.include_in_outcomes)  # type: ignore

    def test_get_supervision_officer_entity_not_included_in_outcomes_from_metrics_table(
        self,
    ) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            # officer fixture where include_in_outcomes=True in the SupervisionOfficer
            # table but include_in_outcomes=False in the SupervisionOfficerMetrics
            # table
            pseudonymized_officer_id="officerhash13",
            category_type_to_compare=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
            include_workflows_info=True,
            num_lookback_periods=0,
        )

        self.assertIsNotNone(actual)
        self.assertFalse(actual.include_in_outcomes)  # type: ignore

    def test_get_supervision_officer_entity_not_included_in_outcomes(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            # officer fixture where include_in_outcomes=False
            pseudonymized_officer_id="officerhash12",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )

        self.assertIsNotNone(actual)
        self.assertFalse(actual.include_in_outcomes)  # type: ignore

    def test_get_supervision_officer_entity_no_match(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_entity(
            pseudonymized_officer_id="invalidhash",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            include_workflows_info=True,
            num_lookback_periods=0,
        )

        self.assertIsNone(actual)

    def test_supervisor_exists_with_external_id_no_match(self) -> None:
        # If matching supervisor doesn't exist, return None
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).supervisor_exists_with_external_id(external_id="invalidid")
        self.assertFalse(actual)

    def test_get_officer_outcomes_found_match(self) -> None:
        # Return matching supervision officer outcomes entity
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=0,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_found_match")  # type: ignore[attr-defined]

    def test_get_officer_outcomes_found_match_with_highlights(self) -> None:
        # Return matching outcomes info where officer should be highlighted for a metric in the latest period
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="officerhash3",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=0,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_found_match_with_highlights")  # type: ignore[attr-defined]

    def test_get_officer_outcomes_found_match_not_top_x_pct(self) -> None:
        # Return matching outcomes info where officer has highlight information, but should not be highlighted
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="officerhash9",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=0,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_found_match_not_top_x_pct")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_officer_outcomes_highlight_in_prev_period_only(
        self, mock_config: MagicMock
    ) -> None:
        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_2(StateCode.US_XX)],
        )

        # Return matching outcomes info where officer is highlighted in a previous period but not the latest
        actual = OutliersQuerier(
            StateCode.US_XX,
            [],  # use empty FV list because the metric with positive highlights configured has an inverseFeatureVariant
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="officerhash7",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=1,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_highlight_in_prev_period_only")  # type: ignore[attr-defined]

    def test_get_officer_outcomes_changing_caseload_categories(self) -> None:
        # Return matching officer outcomes info where officer has changing caseload categories over time
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="officerhash1",
            category_type_to_compare=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
            num_lookback_periods=1,
        )
        self.snapshot.assert_match(actual, name="test_get_officer_outcomes_changing_caseload_categories")  # type: ignore[attr-defined]

    def test_get_officer_outcomes_conflicting_caseload_categories(
        self,
    ) -> None:
        with SessionFactory.using_database(self.insights_database_key) as session:
            new_outlier_status_dict = {
                "state_code": "US_XX",
                "officer_id": "OFFICER9",
                "metric_id": "incarceration_starts_and_inferred",
                "period": "YEAR",
                "end_date": "2023-05-01",
                "metric_rate": "0.333",
                "target": "0.14",
                "threshold": "0.21",
                "status": "FAR",
                "category_type": "SEX_OFFENSE_BINARY",
                "caseload_category": "SEX_OFFENSE",
            }
            new_outlier_status_dict_conflicting_category = new_outlier_status_dict | {
                "metric_id": "absconsions_bench_warrants",
                "caseload_category": "NOT_SEX_OFFENSE",
            }
            session.add(
                SupervisionOfficerOutlierStatus(**new_outlier_status_dict),
                SupervisionOfficerOutlierStatus(
                    **new_outlier_status_dict_conflicting_category
                ),
            )

        with self.assertRaises(ValueError):
            OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            ).get_supervision_officer_outcomes(
                pseudonymized_officer_id="officerhash9",
                category_type_to_compare=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
                num_lookback_periods=1,
            )

    def test_get_officer_outcomes_no_match(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="invalidhash",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=0,
        )

        self.assertIsNone(actual)

    def test_get_officer_outcomes_no_metrics(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            pseudonymized_officer_id="officerhash9",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=0,
            period_end_date=TEST_PREV_END_DATE,
        )

        self.assertIsNone(actual)

    def test_get_officer_outcomes_excluded_from_outcomes(self) -> None:
        # Return None because no valid officer found
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_outcomes(
            # officer fixture where include_in_outcomes=False
            pseudonymized_officer_id="officerhash12",
            category_type_to_compare=InsightsCaseloadCategoryType.ALL,
            num_lookback_periods=0,
            period_end_date=TEST_PREV_END_DATE,
        )

        self.assertIsNone(actual)

    def test_supervisor_exists_with_external_id_found_match(self) -> None:
        # Return matching supervisor
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).supervisor_exists_with_external_id(external_id="101")

        self.assertTrue(actual)  # type: ignore[union-attr]

    def test_get_events_by_client(self) -> None:
        # Return matching event
        with SessionFactory.using_database(self.insights_database_key) as session:
            state_code = StateCode.US_XX
            metric_id = build_test_metric_3(state_code).name
            expected = (
                session.query(SupervisionClientEvent)
                .filter(SupervisionClientEvent.event_date == "2023-05-01")
                .first()
            )

            actual = OutliersQuerier(
                state_code, self.test_user_context.feature_variants
            ).get_events_by_client("clienthash1", [metric_id], TEST_END_DATE)
            self.assertEqual(len(actual), 1)
            self.assertEqual(expected.event_date, actual[0].event_date)
            self.assertEqual(expected.client_id, actual[0].client_id)
            self.assertEqual(metric_id, actual[0].metric_id)

    def test_get_events_by_client_within_lookforward(self) -> None:
        # Return matching event
        with SessionFactory.using_database(self.insights_database_key) as session:
            state_code = StateCode.US_XX
            metric_id = build_test_metric_3(state_code).name
            expected = (
                session.query(SupervisionClientEvent)
                .filter(SupervisionClientEvent.event_date == "2023-05-01")
                .first()
            )

            earlier_end_date = TEST_END_DATE - relativedelta(months=2)
            actual = OutliersQuerier(
                state_code, self.test_user_context.feature_variants
            ).get_events_by_client("clienthash1", [metric_id], earlier_end_date)
            self.assertEqual(len(actual), 1)
            self.assertEqual(expected.event_date, actual[0].event_date)
            self.assertEqual(expected.client_id, actual[0].client_id)
            self.assertEqual(metric_id, actual[0].metric_id)

    def test_get_events_by_client_none_found(self) -> None:
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_events_by_client(
            "randomhash", ["absconsions_bench_warrants"], TEST_END_DATE
        )
        self.assertEqual(actual, [])

    def test_get_supervision_client_no_match(self) -> None:
        # Return None because none found
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_client_from_pseudonymized_id(
            pseudonymized_id="randomhash",
        )

        self.assertIsNone(actual)

    def test_get_supervision_client_success(self) -> None:
        # Return matching supervisor
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_client_from_pseudonymized_id(pseudonymized_id="clienthash1")

        self.assertEqual("111", actual.client_id)  # type: ignore[union-attr]

    def test_get_user_metadata_empty(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )

        self.assertIsNone(querier.get_user_metadata(pseudonymized_id="hash3"))

    def test_get_user_metadata_defaults(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertFalse(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertFalse(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_get_user_metadata_success(self) -> None:
        pid = "hash1"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertTrue(metadata.has_seen_onboarding)  # type: ignore
        self.assertTrue(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertTrue(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_get_user_metadata_data_unavailable_note_dismissed(self) -> None:
        pid = "hash4"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertTrue(metadata.has_seen_onboarding)  # type: ignore
        self.assertTrue(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertFalse(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_get_user_metadata_over_100_note_dismissed(self) -> None:
        pid = "hash5"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertFalse(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertTrue(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_update_user_metadata_new_entity(self) -> None:
        pid = "hash3"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        querier.update_user_metadata(
            pid,
            {
                "has_seen_onboarding": True,
                "has_dismissed_data_unavailable_note": True,
                "has_dismissed_rate_over_100_percent_note": True,
            },
        )
        metadata = querier.get_user_metadata(pid)
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertTrue(metadata.has_seen_onboarding)  # type: ignore
        self.assertTrue(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertTrue(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_update_user_metadata_existing_entity(self) -> None:
        pid = "hash1"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        querier.update_user_metadata(
            pid,
            {
                "has_seen_onboarding": False,
                "has_dismissed_data_unavailable_note": True,
                "has_dismissed_rate_over_100_percent_note": True,
            },
        )
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertTrue(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertTrue(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_update_user_metadata_dismissed_data_unavailable(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        # first, check that all the fields are false so we can later check that only the field we care about changed
        metadata = querier.get_user_metadata(pid)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertFalse(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertFalse(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore
        querier.update_user_metadata(pid, {"has_dismissed_data_unavailable_note": True})
        # has_dismissed_data_unavailable_note should have changed to True, and no other fields should have been changed
        updated_metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(updated_metadata.has_seen_onboarding)  # type: ignore
        self.assertTrue(updated_metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertFalse(updated_metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_update_user_metadata_dismissed_over_100(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        metadata = querier.get_user_metadata(pid)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertFalse(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertFalse(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore
        querier.update_user_metadata(
            pid, {"has_dismissed_rate_over_100_percent_note": True}
        )
        # has_dismissed_rate_over_100_percent_note should have changed to True, and no other fields should have been changed
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertFalse(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertTrue(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_update_user_metadata_dismissed_both_notes(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        querier.update_user_metadata(
            pid,
            {
                "has_dismissed_data_unavailable_note": True,
                "has_dismissed_rate_over_100_percent_note": True,
            },
        )
        metadata = querier.get_user_metadata(pid)
        self.assertIsNotNone(metadata)
        self.assertFalse(metadata.has_seen_onboarding)  # type: ignore
        self.assertTrue(metadata.has_dismissed_data_unavailable_note)  # type: ignore
        self.assertTrue(metadata.has_dismissed_rate_over_100_percent_note)  # type: ignore

    def test_get_user_info_supervision_officer_supervisor_success(self) -> None:
        pid = "hash1"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_supervisor_user_info(pid)

        self.assertIsNotNone(result.entity)
        self.assertEqual(result.entity.external_id, "101")  # type: ignore
        self.assertEqual(result.role, "supervision_officer_supervisor")
        self.assertTrue(result.has_seen_onboarding)
        self.assertTrue(result.has_dismissed_data_unavailable_note)
        self.assertTrue(result.has_dismissed_rate_over_100_percent_note)

    def test_get_user_info_supervision_officer_success(self) -> None:
        pid = "officerhash1"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        workflows_info = True
        category_type_to_compare = InsightsCaseloadCategoryType.ALL
        result = querier.get_officer_user_info(
            pid, workflows_info, category_type_to_compare
        )

        self.assertIsNotNone(result.entity)
        self.assertEqual(result.entity.external_id, "OFFICER1")  # type: ignore
        self.assertEqual(result.role, "supervision_officer")
        self.assertTrue(result.has_seen_onboarding)
        self.assertFalse(result.has_dismissed_data_unavailable_note)
        self.assertFalse(result.has_dismissed_rate_over_100_percent_note)

    def test_get_user_info_default_onboarding(self) -> None:
        pid = "hash2"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_supervisor_user_info(pid)

        self.assertIsNotNone(result.entity)
        self.assertEqual(result.entity.external_id, "102")  # type: ignore
        self.assertEqual(result.role, "supervision_officer_supervisor")
        self.assertFalse(result.has_seen_onboarding)
        self.assertFalse(result.has_dismissed_data_unavailable_note)
        self.assertFalse(result.has_dismissed_rate_over_100_percent_note)

    def test_get_user_info_non_supervisor(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_supervisor_user_info("leadership-hash")

        self.assertEqual(
            result,
            UserInfo(
                entity=None,
                role=None,
                has_seen_onboarding=True,
                has_dismissed_data_unavailable_note=False,
                has_dismissed_rate_over_100_percent_note=False,
            ),
        )

    def test_get_configuration_for_user_no_fv(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        user_context = None
        result = querier.get_configuration_for_user(user_context)

        self.assertEqual(result.id, 1)

    def test_get_configuration_for_user_fv(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        user_context = UserContext(
            email_address="tester@example.com",
            state_code_str="US_XX",
            user_external_id="id",
            pseudonymized_id="hash",
            can_access_all_supervisors=True,
            can_access_supervision_workflows=True,
            feature_variants=["fv1", "random"],
        )
        result = querier.get_configuration_for_user(user_context)

        self.assertEqual(result.id, 3)

    def test_get_configuration_for_user_no_fv_match(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        user_context = UserContext(
            email_address="tester@example.com",
            state_code_str="US_XX",
            user_external_id="id",
            pseudonymized_id="hash",
            can_access_all_supervisors=True,
            can_access_supervision_workflows=True,
            feature_variants=["randomfv"],
        )
        result = querier.get_configuration_for_user(user_context)

        self.assertEqual(result.id, 1)

    def test_get_configuration_for_user_multiple_fv(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        user_context = UserContext(
            email_address="tester@example.com",
            state_code_str="US_XX",
            user_external_id="id",
            pseudonymized_id="hash",
            can_access_all_supervisors=True,
            can_access_supervision_workflows=True,
            feature_variants=["fv1", "fv2"],
        )

        with patch("logging.Logger.error") as mock_logger:
            result = querier.get_configuration_for_user(user_context)

            self.assertEqual(result.id, 1)
            mock_logger.assert_has_calls(
                [
                    call(
                        "Multiple configurations and feature variants may apply to this user, however only one should apply. The default configuration will be shown instead."
                    )
                ]
            )

    def test_get_configurations(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_configurations()

        self.assertListEqual([config.id for config in result], [1, 4, 3, 2, 5])

    def test_add_configuration_no_fv(self) -> None:
        new_datetime = datetime(2024, 2, 1)
        config_to_add = {
            "updated_at": new_datetime,
            "feature_variant": None,
            "updated_by": "alexa@recidiviz.org",
            "supervision_district_label": "district",
            "supervision_supervisor_label": "supervisor",
            "supervision_jii_label": "client",
            "supervisor_has_no_outlier_officers_label": "Nice! No officers are outliers on any metrics this month.",
            "officer_has_no_outlier_metrics_label": "Nice! No outlying metrics this month.",
            "supervisor_has_no_officers_with_eligible_clients_label": "Nice! No outstanding opportunities for now.",
            "officer_has_no_eligible_clients_label": "Nice! No outstanding opportunities for now.",
            "supervision_officer_label": "officer",
            "supervision_unit_label": "unit",
            "supervision_district_manager_label": "district manager",
            "learn_more_url": "fake.com",
            "status": ConfigurationStatus.ACTIVE.value,
            "none_are_outliers_label": "are outliers",
            "worse_than_rate_label": "Far worse than statewide rate",
            "slightly_worse_than_rate_label": "slightly worse than statewide rate",
            "at_or_below_rate_label": "At or below statewide rate",
            "exclusion_reason_description": "excluded because x",
            "vitals_metrics_methodology_url": "http://example.com/methodology",
        }

        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        querier.add_configuration(config_to_add)

        results = querier.get_configurations()
        self.assertEqual(results[0].feature_variant, None)
        self.assertEqual(results[0].updated_by, "alexa@recidiviz.org")
        # Assumes that the first non-header row in configurations.csv has feature_variant=None
        self.assertEqual(
            querier.get_configuration(1).status, ConfigurationStatus.INACTIVE.value
        )

    def test_add_configuration_fv(self) -> None:
        config_to_add = {
            "updated_at": datetime(2024, 2, 1),
            "feature_variant": "fv1",
            "updated_by": "alexa@recidiviz.org",
            "supervision_district_label": "district",
            "supervision_supervisor_label": "supervisor",
            "supervision_jii_label": "client",
            "supervisor_has_no_outlier_officers_label": "Nice! No officers are outliers on any metrics this month.",
            "officer_has_no_outlier_metrics_label": "Nice! No outlying metrics this month.",
            "supervisor_has_no_officers_with_eligible_clients_label": "Nice! No outstanding opportunities for now.",
            "officer_has_no_eligible_clients_label": "Nice! No outstanding opportunities for now.",
            "supervision_officer_label": "officer",
            "supervision_unit_label": "unit",
            "supervision_district_manager_label": "district manager",
            "learn_more_url": "fake.com",
            "status": ConfigurationStatus.ACTIVE.value,
            "none_are_outliers_label": "are outliers",
            "worse_than_rate_label": "Far worse than statewide rate",
            "slightly_worse_than_rate_label": "slightly worse than statewide rate",
            "at_or_below_rate_label": "At or below statewide rate",
            "exclusion_reason_description": "excluded because x",
            "vitals_metrics_methodology_url": "http://example.com/methodology",
        }

        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        querier.add_configuration(config_to_add)

        results = querier.get_configurations()
        self.assertEqual(results[0].feature_variant, "fv1")
        self.assertEqual(results[0].updated_by, "alexa@recidiviz.org")
        # Assumes that the third non-header row in configurations.csv has feature_variant=fv1
        self.assertEqual(
            querier.get_configuration(3).status, ConfigurationStatus.INACTIVE.value
        )

    def test_add_configuration_error(self) -> None:
        config_to_add = {
            "updated_at": datetime(2024, 2, 1),
            "feature_variant": "fv1",
            "supervision_district_label": "district",
            "supervision_supervisor_label": "supervisor",
            "supervision_jii_label": "client",
            "supervisor_has_no_outlier_officers_label": "Nice! No officers are outliers on any metrics this month.",
            "officer_has_no_outlier_metrics_label": "Nice! No outlying metrics this month.",
            "supervisor_has_no_officers_with_eligible_clients_label": "Nice! No outstanding opportunities for now.",
            "officer_has_no_eligible_clients_label": "Nice! No outstanding opportunities for now.",
            "supervision_officer_label": "officer",
            "supervision_unit_label": "unit",
            "supervision_district_manager_label": "district manager",
            "learn_more_url": "fake.com",
            "status": ConfigurationStatus.ACTIVE.value,
        }

        with self.assertRaises(IntegrityError):
            querier = OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            )
            querier.add_configuration(config_to_add)
            # Assert the latest entity is the first non-header row in configurations.csv
            self.assertEqual(querier.get_configurations()[0].id, 1)

    def test_deactivate_configuration_no_matching_id(self) -> None:
        with self.assertRaises(NoResultFound):
            querier = OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            )
            querier.deactivate_configuration(1001)

    def test_deactivate_configuration_inactive(self) -> None:
        with patch("logging.Logger.error") as mock_logger:
            querier = OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            )
            # Assumes that the second non-header row in configurations.csv has status=INACTIVE
            querier.deactivate_configuration(2)
            mock_logger.assert_has_calls(
                [call("Configuration %s is already inactive", 2)]
            )

    def test_deactivate_configuration_default_config(self) -> None:
        with self.assertRaises(ValueError):
            querier = OutliersQuerier(
                StateCode.US_XX, self.test_user_context.feature_variants
            )
            # Assumes that the first non-header row in configurations.csv is the only
            # row with feature_variant=None and status=ACTIVE
            querier.deactivate_configuration(1)

    def test_deactivate_configuration_success(self) -> None:
        # Assumes that the third non-header row in configurations.csv has
        # feature_variant != None and status=ACTIVE
        config_id_to_deactivate = 3
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        querier.deactivate_configuration(config_id_to_deactivate)

        self.assertEqual(
            querier.get_configuration(config_id_to_deactivate).status,
            ConfigurationStatus.INACTIVE.value,
        )

    def test_get_product_configuration(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_product_configuration(user_context=self.test_user_context)
        self.snapshot.assert_match(result, name="test_get_product_configuration")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_product_configuration_with_specialized_category_type(
        self, mock_config: MagicMock
    ) -> None:
        state_code = StateCode.US_XX
        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code), build_test_metric_2(state_code)],
            available_specialized_caseload_categories={
                InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY: [
                    CaseloadCategory(
                        id=StateStaffCaseloadType.SEX_OFFENSE.name,
                        display_name="Sex Offense Caseload",
                    ),
                    CaseloadCategory(
                        id=f"NOT_{StateStaffCaseloadType.SEX_OFFENSE.name}",
                        display_name="General + Other Caseloads",
                    ),
                ]
            },
            primary_category_type=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
        )
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)
        result = querier.get_product_configuration(user_context=self.test_user_context)
        self.snapshot.assert_match(result, name="test_get_product_configuration_with_specialized_category_type")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_product_configuration_invalid_primary_category_type(
        self, mock_config: MagicMock
    ) -> None:
        state_code = StateCode.US_XX
        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code), build_test_metric_2(state_code)],
            available_specialized_caseload_categories={},
            primary_category_type=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
        )
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)
        with self.assertRaisesRegex(ValueError, "Invalid product configuration"):
            querier.get_product_configuration(user_context=self.test_user_context)

    def test_get_action_strategy_surfaced_events_for_supervisor(self) -> None:
        pseudo_id = "hash1"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_action_strategy_surfaced_events_for_supervisor(
            supervisor_pseudonymized_id=pseudo_id
        )
        self.snapshot.assert_match(result, name="test_get_action_strategy_surfaced_events_for_supervisor")  # type: ignore[attr-defined]

    def test_get_most_recent_action_strategy_surfaced_event_for_supervisor(
        self,
    ) -> None:
        pseudo_id = "hash1"
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        result = querier.get_most_recent_action_strategy_surfaced_event_for_supervisor(
            supervisor_pseudonymized_id=pseudo_id
        )
        self.snapshot.assert_match(result, name="test_get_most_recent_action_strategy_surfaced_event_for_supervisor")  # type: ignore[attr-defined]

    def test_insert_action_strategy_surfaced_event_success(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        pseudo_id = "hash1"
        event = ActionStrategySurfacedEvent(
            state_code="US_XX",
            user_pseudonymized_id=pseudo_id,
            officer_pseudonymized_id="officer_hash",
            action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
            timestamp=datetime.now().date(),
        )
        querier.insert_action_strategy_surfaced_event(event=event)

        new_event = (
            querier.get_most_recent_action_strategy_surfaced_event_for_supervisor(
                supervisor_pseudonymized_id=pseudo_id
            )
        )

        self.assertIsNotNone(new_event)
        self.assertTrue(new_event.user_pseudonymized_id == event.user_pseudonymized_id)
        self.assertTrue(
            new_event.officer_pseudonymized_id == event.officer_pseudonymized_id
        )
        self.assertTrue(new_event.action_strategy == event.action_strategy)
        self.assertTrue(new_event.timestamp == event.timestamp)

    def test_insert_action_strategy_surfaced_event_duplicate_failure(self) -> None:
        querier = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        )
        pseudo_id = "hash1"
        event = ActionStrategySurfacedEvent(
            state_code="US_XX",
            user_pseudonymized_id=pseudo_id,
            officer_pseudonymized_id="officer_hash",
            action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
            timestamp=datetime.now(),
        )
        querier.insert_action_strategy_surfaced_event(event=event)

        # Insert same event twice should raise error
        with self.assertRaises(IntegrityError):
            querier.insert_action_strategy_surfaced_event(event=event)

    def test_get_officer_from_pseudo_id_found_match(self) -> None:
        # Return matching officer
        test_id = "officerhash4"
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_from_pseudonymized_id(pseudonymized_id=test_id)

        self.assertEqual(test_id, actual.pseudonymized_id)  # type: ignore[union-attr]

    def test_get_officer_from_pseudo_id_no_match(self) -> None:
        # If matching officer doesn't exist, return None
        actual = OutliersQuerier(
            StateCode.US_XX, self.test_user_context.feature_variants
        ).get_supervision_officer_from_pseudonymized_id(pseudonymized_id="doesnotexist")
        self.assertIsNone(actual)

    def test_get_all_supervision_officers_required_info_only(self) -> None:
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)
        result = querier.get_all_supervision_officers_required_info_only()

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_all_supervision_officers_required_info_only"
        )

    def test_get_supervision_officers_by_external_ids(self) -> None:

        TEST_CASES = [
            ("one valid external_id", ["OFFICER1"], ["OFFICER1"]),
            ("one invalid external_id", ["some_bad_external_id"], []),
            (
                "one invalid external_id, one valid_external_id",
                ["OFFICER2", "bad_external_id"],
                ["OFFICER2"],
            ),
            (
                "all valid external_ids",
                ["OFFICER1", "OFFICER2"],
                ["OFFICER1", "OFFICER2"],
            ),
            ("no external_id", [], []),
        ]

        for test_message, external_ids, expected_external_ids in TEST_CASES:

            with self.subTest(test_message):
                actual = OutliersQuerier(
                    StateCode.US_XX, self.test_user_context.feature_variants
                ).get_supervision_officers_by_external_ids(external_ids=external_ids)
                self.assertCountEqual(
                    expected_external_ids, [o.external_id for o in actual]
                )

    def test_get_supervision_officer_supervisors_by_external_ids(self) -> None:

        TEST_CASES = [
            ("one valid external_id", ["101"], ["101"]),
            ("one invalid external_id", ["some_bad_external_id"], []),
            (
                "one invalid external_id, one valid_external_id",
                ["102", "bad_external_id"],
                ["102"],
            ),
            ("all valid external_ids", ["101", "102"], ["101", "102"]),
            ("no external_id", [], []),
        ]

        for test_message, external_ids, expected_external_ids in TEST_CASES:

            with self.subTest(test_message):
                actual = OutliersQuerier(
                    StateCode.US_XX, self.test_user_context.feature_variants
                ).get_supervision_officer_supervisors_by_external_ids(
                    external_ids=external_ids
                )
                self.assertCountEqual(
                    expected_external_ids, [o.external_id for o in actual]
                )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_officer_vitals_metrics(self, mock_config: MagicMock) -> None:
        pseudo_id = "officerhash2"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_officer_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_supervisor_vitals_metrics(self, mock_config: MagicMock) -> None:
        supervisor_pseudonymized_id = "hash1"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result: list[
            VitalsMetric
        ] = querier.get_vitals_metrics_for_supervision_officer_supervisor(
            supervisor_pseudonymized_id=supervisor_pseudonymized_id,
        )
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_supervisor_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_supervisor_can_access_all_supervisors_vitals_metrics(
        self, mock_config: MagicMock
    ) -> None:
        supervisor_pseudonymized_id = "hash1"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result: list[
            VitalsMetric
        ] = querier.get_vitals_metrics_for_supervision_officer_supervisor(
            supervisor_pseudonymized_id=supervisor_pseudonymized_id,
            can_access_all_supervisors=True,
        )
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_supervisor_can_access_all_supervisors_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_supervisor_with_no_officers_vitals_metrics(
        self, mock_config: MagicMock
    ) -> None:
        supervisor_pseudonymized_id = "hash5"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result: list[
            VitalsMetric
        ] = querier.get_vitals_metrics_for_supervision_officer_supervisor(
            supervisor_pseudonymized_id=supervisor_pseudonymized_id,
        )
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_supervisor_with_no_officers_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_missing_latest_metric_value_vitals_metrics(
        self, mock_config: MagicMock
    ) -> None:
        pseudo_id = "officerhash3"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_missing_latest_metric_value_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_missing_previous_metric_value_vitals_metrics(
        self, mock_config: MagicMock
    ) -> None:
        pseudo_id = "officerhash4"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_missing_previous_metric_value_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_missing_previous_date_vitals_metric(
        self, mock_config: MagicMock
    ) -> None:
        pseudo_id = "officerhash5"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_missing_previous_date_vitals_metric"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_no_vitals_metrics(self, mock_config: MagicMock) -> None:
        pseudo_id = "officerhash6"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="test_get_no_vitals_metrics"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_missing_latest_date_and_value_vitals_metric(
        self, mock_config: MagicMock
    ) -> None:
        pseudo_id = "officerhash7"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_missing_latest_date_and_value_vitals_metric"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_first_of_month_date_officer_vitals_metric(
        self, mock_config: MagicMock
    ) -> None:
        pseudo_id = "officerhash8"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_first_of_month_date_officer_vitals_metric"
        )

    @freezegun.freeze_time(datetime(2025, 6, 14, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_vitals_metric_with_month_period(self, mock_config: MagicMock) -> None:
        pseudo_id = "officerhash9"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT_DUE_DATE_BASED],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_vitals_metric_with_month_period"
        )

    @freezegun.freeze_time(datetime(2025, 3, 2, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_vitals_metric_with_short_month(self, mock_config: MagicMock) -> None:
        # Edge case when the previous month was shorter than 30 days, so the previous metric
        # end dates for MONTH and DAY period metrics are in different months.
        pseudo_id = "officerhash10"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT_DUE_DATE_BASED, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_vitals_metric_with_short_month"
        )

    @freezegun.freeze_time(datetime(2025, 7, 31, 0, 0, 0, 0))
    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_vitals_metric_with_long_month(self, mock_config: MagicMock) -> None:
        # Edge case when the previous month was longer than 30 days, so the previous metric
        # end dates for MONTH and DAY period metrics are in different months.
        pseudo_id = "officerhash11"
        state_code = StateCode.US_XX
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(state_code)],
            vitals_metrics=[TIMELY_CONTACT_DUE_DATE_BASED, TIMELY_RISK_ASSESSMENT],
        )

        result = querier.get_vitals_metrics_for_supervision_officer(pseudo_id)

        self.snapshot.assert_match(  # type: ignore[attr-defined]
            result, name="get_vitals_metric_with_long_month"
        )

    @patch(
        "recidiviz.outliers.querier.querier.OutliersQuerier.get_outliers_backend_config"
    )
    def test_get_outcomes_metrics(self, mock_config: MagicMock) -> None:
        state_code = StateCode.US_PA
        querier = OutliersQuerier(state_code, self.test_user_context.feature_variants)

        metric_1 = build_test_metric_1(state_code)  # neither set
        metric_2 = build_test_metric_2(state_code)  # inverse_feature_variant set
        metric_3 = build_test_metric_3(state_code)  # feature_variant set

        mock_config.return_value = OutliersBackendConfig(
            metrics=[metric_1, metric_2, metric_3],
        )

        metrics = querier.get_outcomes_metrics()
        self.assertCountEqual([metric_1, metric_3], metrics)

    def test_metric_with_feature_variant_and_inverse_set(self) -> None:
        with self.assertRaises(TypeError):
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_XX,
                metric=INCARCERATION_STARTS_AND_INFERRED,
                title_display_name="Incarceration Rate (CPVs & TPVs)",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
                description_markdown="""Incarceration rate description

        <br />
        Incarceration rate denominator description""",
                feature_variant=TEST_FEATURE_VARIANT,
                inverse_feature_variant=TEST_FEATURE_VARIANT,
            )
