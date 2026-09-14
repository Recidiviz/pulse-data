# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests the DatasetCleanupAndValidationEntrypoint."""
import datetime
import unittest
from unittest.mock import MagicMock, Mock, patch

from google.cloud.bigquery import Dataset, SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint import (
    EMPTY_DATASET_DELETION_MIN_SECONDS,
    NON_EMPTY_TEMP_DATASET_DELETION_MIN_SECONDS,
    DatasetCleanupAndValidationEntrypoint,
)
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_data_temp_load_dataset,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
    SourceTableUpdateGroup,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.big_query_view_test_utils import MINIMAL_SCHEMA

_FAKE_MANAGED_VIEW_BUILDER_A = SimpleBigQueryViewBuilder(
    dataset_id="fake_managed_dataset_a",
    view_id="fake_view_a",
    description="Fake managed view used to exercise cleanup dispatch in tests.",
    view_query_template="SELECT NULL LIMIT 0",
    schema=MINIMAL_SCHEMA,
)
_FAKE_MANAGED_VIEW_BUILDER_B = SimpleBigQueryViewBuilder(
    dataset_id="fake_managed_dataset_b",
    view_id="fake_view_b",
    description="Fake managed view used to exercise cleanup dispatch in tests.",
    view_query_template="SELECT NULL LIMIT 0",
    schema=MINIMAL_SCHEMA,
)
_FAKE_MANAGED_DATASETS = {"fake_managed_dataset_a", "fake_managed_dataset_b"}


def _build_fake_managed_dag_walker() -> BigQueryViewDagWalker:
    return BigQueryViewDagWalker(
        views=[
            _FAKE_MANAGED_VIEW_BUILDER_A.build(),
            _FAKE_MANAGED_VIEW_BUILDER_B.build(),
        ]
    )


_DATASET_CREATION_AGES = {
    "beam_temp_dataset_": datetime.timedelta(days=24),
    "scratch_test_empty_dataset": datetime.timedelta(days=1),
    "test_recent_empty_dataset": datetime.timedelta(
        seconds=EMPTY_DATASET_DELETION_MIN_SECONDS - 15
    ),
    "temp_dataset_recent_empty": datetime.timedelta(
        seconds=EMPTY_DATASET_DELETION_MIN_SECONDS - 15
    ),
    "temp_dataset_recent_non_empty": datetime.timedelta(
        seconds=NON_EMPTY_TEMP_DATASET_DELETION_MIN_SECONDS - 15
    ),
}

_DATASET_LABELS = {"terraform_managed_dataset": {"managed_by_terraform": "true"}}


def _creation_time_fake(
    _self: MagicMock, dataset: Dataset, _cls: type[Dataset]
) -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc) - _DATASET_CREATION_AGES.get(
        dataset.dataset_id, datetime.timedelta(days=30)
    )


def _labels_fake(_self: MagicMock, dataset: Dataset, _cls: type[Dataset]) -> dict:
    return _DATASET_LABELS.get(dataset.dataset_id, {})


class DatasetCleanupAndValidationEntrypointTest(BigQueryEmulatorTestCase):
    """Tests for DatasetCleanupAndValidationEntrypointTest"""

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return [
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id="terraform_managed_dataset",
                description="Terraform managed",
                update_config=SourceTableCollectionUpdateConfig.protected(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id=raw_data_pruning_new_raw_data_dataset(
                    StateCode.US_AZ, DirectIngestInstance.PRIMARY
                ),
                description="Test dataset for pruning",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id=raw_data_temp_load_dataset(
                    StateCode.US_AZ, DirectIngestInstance.PRIMARY
                ),
                description="Test dataset for temp loading",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id="beam_temp_dataset",
                description="Test dataset for beam temp tables",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id="test_empty_dataset",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id="temp_dataset_recent_empty",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id="test_recent_empty_dataset",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                update_groups={SourceTableUpdateGroup.CALC},
                dataset_id="temp_dataset_recent_non_empty",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
                source_tables_by_address={
                    BigQueryAddress(
                        dataset_id="temp_dataset_recent_non_empty",
                        table_id="test_table",
                    ): SourceTableConfig(
                        address=BigQueryAddress(
                            dataset_id="temp_dataset_recent_non_empty",
                            table_id="test_table",
                        ),
                        description="Test table",
                        schema_fields=[SchemaField("name", "STRING", "NULLABLE")],
                    )
                },
            ),
        ]

    @patch(
        "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED",
        _FAKE_MANAGED_DATASETS,
    )
    @patch(
        "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.build_dag_walker_for_all_deployed_view_graphs",
        new=_build_fake_managed_dag_walker,
    )
    @patch(
        "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.validate_clean_source_table_datasets",
    )
    @patch(
        "google.cloud.bigquery.dataset.Dataset.created",
    )
    @patch(
        "google.cloud.bigquery.dataset.Dataset.labels",
    )
    @patch(
        "google.cloud.bigquery.client.Client.list_routines",
        return_value=[],
    )
    def test_entrypoint(
        self,
        _mock_routines: Mock,
        _mock_labels: Mock,
        _mock_created: Mock,
        _mock_validate: Mock,
    ) -> None:
        """Test that _delete_empty_or_temp_datasets does:
        - not delete a dataset if it has tables in it
        - not delete an empty dataset if it is managed by Terraform
        - deletes a non-empty dataset if it was created by a Beam pipeline more than 24 hours ago.
        - does not delete raw data pruning datasets we expect to be empty sometimes
        - does not delete raw data temp load datasets we expect to be empty sometimes

        The view graph backing cleanup of unmanaged views/datasets is faked out with a
        couple of views in datasets that don't exist in the emulator, so that cleanup
        logs and skips them rather than exercising the real (large, slow) view graph.
        """

        _mock_created.__get__ = _creation_time_fake
        _mock_labels.__get__ = _labels_fake

        all_datasets = [
            source_table_collection.dataset_id
            for source_table_collection in self.get_source_tables()
        ]
        expected_deleted_datasets = ["beam_temp_dataset", "test_empty_dataset"]

        assert sorted(
            [dataset.dataset_id for dataset in self.bq_client.list_datasets()]
        ) == sorted(all_datasets)

        args = DatasetCleanupAndValidationEntrypoint.get_parser().parse_args([])
        DatasetCleanupAndValidationEntrypoint.run_entrypoint(args=args)

        assert sorted(
            [dataset.dataset_id for dataset in self.bq_client.list_datasets()]
        ) == sorted(set(all_datasets) - set(expected_deleted_datasets))


_EXPECTED_FAKE_MANAGED_VIEWS_MAP = {
    "fake_managed_dataset_a": {
        BigQueryAddress(dataset_id="fake_managed_dataset_a", table_id="fake_view_a")
    },
    "fake_managed_dataset_b": {
        BigQueryAddress(dataset_id="fake_managed_dataset_b", table_id="fake_view_b")
    },
}


class DatasetCleanupAndValidationEntrypointCleanupDispatchTest(unittest.TestCase):
    """Tests that run_entrypoint dispatches cleanup of unmanaged views/datasets using
    the managed view map derived from the deployed view graph registry.
    """

    def setUp(self) -> None:
        self.mock_bq_client = MagicMock()

        client_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.BigQueryClientImpl",
            return_value=self.mock_bq_client,
        )
        client_patcher.start()
        self.addCleanup(client_patcher.stop)

        dag_walker_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.build_dag_walker_for_all_deployed_view_graphs",
            new=_build_fake_managed_dag_walker,
        )
        dag_walker_patcher.start()
        self.addCleanup(dag_walker_patcher.stop)

        managed_datasets_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED",
            new=_FAKE_MANAGED_DATASETS,
        )
        managed_datasets_patcher.start()
        self.addCleanup(managed_datasets_patcher.stop)

        cleanup_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.cleanup_datasets_and_delete_unmanaged_views"
        )
        self.mock_cleanup = cleanup_patcher.start()
        self.addCleanup(cleanup_patcher.stop)

        delete_empty_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint._delete_empty_or_temp_datasets"
        )
        delete_empty_patcher.start()
        self.addCleanup(delete_empty_patcher.stop)

        validate_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.validate_clean_source_table_datasets"
        )
        validate_patcher.start()
        self.addCleanup(validate_patcher.stop)

        repository_patcher = patch(
            "recidiviz.entrypoints.bigquery.dataset_cleanup_and_validation_entrypoint.build_source_table_repository_for_collected_schemata"
        )
        repository_patcher.start()
        self.addCleanup(repository_patcher.stop)

        project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-staging"
        )
        project_id_patcher.start()
        self.addCleanup(project_id_patcher.stop)

    def test_cleanup_called_once_with_dry_run_false(self) -> None:
        args = DatasetCleanupAndValidationEntrypoint.get_parser().parse_args([])
        DatasetCleanupAndValidationEntrypoint.run_entrypoint(args=args)

        self.mock_cleanup.assert_called_once_with(
            bq_client=self.mock_bq_client,
            managed_views_map=_EXPECTED_FAKE_MANAGED_VIEWS_MAP,
            datasets_that_have_ever_been_managed=_FAKE_MANAGED_DATASETS,
            dry_run=False,
        )

    def test_cleanup_called_once_with_dry_run_true(self) -> None:
        args = DatasetCleanupAndValidationEntrypoint.get_parser().parse_args(
            ["--dry-run"]
        )
        DatasetCleanupAndValidationEntrypoint.run_entrypoint(args=args)

        self.mock_cleanup.assert_called_once_with(
            bq_client=self.mock_bq_client,
            managed_views_map=_EXPECTED_FAKE_MANAGED_VIEWS_MAP,
            datasets_that_have_ever_been_managed=_FAKE_MANAGED_DATASETS,
            dry_run=True,
        )
