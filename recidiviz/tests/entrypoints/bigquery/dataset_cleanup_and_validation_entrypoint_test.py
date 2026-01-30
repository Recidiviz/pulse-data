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
from unittest.mock import MagicMock, Mock, patch

from google.cloud.bigquery import Dataset, SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
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
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
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
                dataset_id="terraform_managed_dataset",
                description="Terraform managed",
                update_config=SourceTableCollectionUpdateConfig.protected(),
            ),
            SourceTableCollection(
                dataset_id=raw_data_pruning_new_raw_data_dataset(
                    StateCode.US_AZ, DirectIngestInstance.PRIMARY
                ),
                description="Test dataset for pruning",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                dataset_id=raw_data_temp_load_dataset(
                    StateCode.US_AZ, DirectIngestInstance.PRIMARY
                ),
                description="Test dataset for temp loading",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                dataset_id="beam_temp_dataset",
                description="Test dataset for beam temp tables",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                dataset_id="test_empty_dataset",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                dataset_id="temp_dataset_recent_empty",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
                dataset_id="test_recent_empty_dataset",
                description="Test dataset for empty dataset",
                update_config=SourceTableCollectionUpdateConfig.regenerable(),
            ),
            SourceTableCollection(
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
