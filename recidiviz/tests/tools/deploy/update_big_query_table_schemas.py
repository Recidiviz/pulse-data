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
"""Tests the recidiviz/tools/deploy/update_big_query_table_schemas.py file that issues updates to raw regional
schemas"""

import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables.source_table_config import (
    RawDataSourceTableLabel,
    SourceTableCollection,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.tools.deploy.update_big_query_table_schemas import (
    update_all_source_table_schemas,
)


@mock.patch("recidiviz.utils.metadata.project_id", return_value="test-project")
def test_create_datasets_if_necessary(_project_id_patch: mock.Mock) -> None:
    mock_bq_client = mock.MagicMock()
    mock_bq_client.dataset_ref_for_id.side_effect = lambda x: x
    source_table_repository = SourceTableRepository(
        source_table_collections=[
            SourceTableCollection(
                dataset_id="us_xx_primary",
                labels=[
                    RawDataSourceTableLabel(
                        state_code=StateCode.US_XX,
                        ingest_instance=DirectIngestInstance.PRIMARY,
                    )
                ],
            )
        ]
    )
    update_all_source_table_schemas(
        source_table_repository=source_table_repository,
        client=mock_bq_client,
    )
    assert mock_bq_client.dataset_ref_for_id.mock_calls == [
        mock.call("us_xx_primary"),
    ]
    assert mock_bq_client.create_dataset_if_necessary.mock_calls == [
        mock.call(dataset_ref="us_xx_primary", default_table_expiration_ms=None),
    ]
