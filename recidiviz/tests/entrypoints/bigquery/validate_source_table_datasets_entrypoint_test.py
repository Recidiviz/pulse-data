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
"""Tests for ValidateSourceTableDatasetsEntrypoint"""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.entrypoints.bigquery.validate_source_table_datasets_entrypoint import (
    ValidateSourceTableDatasetsEntrypoint,
)
from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.utils.airflow_dag import AirflowDag

_PROJECT_ID = "recidiviz-staging"

_ENTRYPOINT_MODULE = (
    "recidiviz.entrypoints.bigquery.validate_source_table_datasets_entrypoint"
)


@patch(f"{_ENTRYPOINT_MODULE}.BigQueryClientImpl")
@patch(f"{_ENTRYPOINT_MODULE}.build_source_table_repository_for_collected_schemata")
@patch(f"{_ENTRYPOINT_MODULE}.validate_clean_source_table_datasets")
@patch("recidiviz.utils.metadata.project_id", return_value=_PROJECT_ID)
class ValidateSourceTableDatasetsEntrypointTest(unittest.TestCase):
    """Tests for ValidateSourceTableDatasetsEntrypoint"""

    @patch(f"{_ENTRYPOINT_MODULE}.in_airflow_kubernetes_pod", return_value=False)
    def test_raises_outside_kubernetes_pod(
        self,
        _mock_in_pod: MagicMock,
        _mock_project_id: MagicMock,
        mock_validate: MagicMock,
        _mock_build_repository: MagicMock,
        _mock_bq_client: MagicMock,
    ) -> None:
        args = ValidateSourceTableDatasetsEntrypoint.get_parser().parse_args([])
        with self.assertRaisesRegex(
            RuntimeError,
            r"^This entrypoint may only be run within the Airflow DAG's "
            r"KubernetesPodOperator\.$",
        ):
            ValidateSourceTableDatasetsEntrypoint.run_entrypoint(args=args)
        mock_validate.assert_not_called()

    @patch(f"{_ENTRYPOINT_MODULE}.in_airflow_kubernetes_pod", return_value=True)
    @patch(
        f"{_ENTRYPOINT_MODULE}.AirflowKubernetesPodEnvironment.get_dag_id",
        return_value=AirflowDag.IDENTITY_INGEST.dag_id(_PROJECT_ID),
    )
    def test_validates_repository_filtered_to_running_dags_update_group(
        self,
        _mock_get_dag_id: MagicMock,
        _mock_in_pod: MagicMock,
        _mock_project_id: MagicMock,
        mock_validate: MagicMock,
        mock_build_repository: MagicMock,
        mock_bq_client: MagicMock,
    ) -> None:
        args = ValidateSourceTableDatasetsEntrypoint.get_parser().parse_args([])
        ValidateSourceTableDatasetsEntrypoint.run_entrypoint(args=args)

        repository = mock_build_repository.return_value
        repository.filter_to_update_group.assert_called_once_with(
            SourceTableUpdateGroup.IDENTITY_INGEST
        )
        mock_validate.assert_called_once_with(
            bq_client=mock_bq_client.return_value,
            source_table_repository=repository.filter_to_update_group.return_value,
        )
